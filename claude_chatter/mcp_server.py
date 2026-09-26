"""
MCP stdio entry point built on FastMCP. FastMCP owns the JSON-RPC/stdio
transport, tool schema generation, and standard notifications; this module
wires ClaudeMessagingProtocol into it and holds nothing else.
"""
import os
import json
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

from fastmcp import FastMCP, Context
from fastmcp.exceptions import ToolError

from .protocol import ClaudeMessagingProtocol

logger = logging.getLogger("SocketBridgeMCP")

INSTRUCTIONS = (
    "Bi-Directional AI Communication MCP Bridge for Claude Code sessions. "
    "When you receive a tools/list_changed notification, call get_responses "
    "(or list_sessions) to see what changed - inbound messages arrive that "
    "way, not inline in the notification itself."
)


def build_mcp_server(protocol: ClaudeMessagingProtocol) -> FastMCP:
    """
    Builds a FastMCP server wired to the given protocol instance. A factory
    rather than a module-level singleton so each process's protocol gets its
    own server with tools closed over it.
    """
    # Holds the most recently seen ServerSession so inbound socket activity
    # (which happens outside any tool call's request context) can still poke
    # the connected host. FastMCP's notification API only supports the
    # standard MCP notification set - there is no way to send our own custom
    # method/params through it, so this uses the standard tools/list_changed
    # signal instead; see INSTRUCTIONS for what the host is expected to do
    # with it.
    last_session_holder: Dict[str, Any] = {"session": None}
    # asyncio.create_task() only holds a WEAK reference to the task it returns -
    # an unreferenced task can be garbage-collected mid-flight, before it
    # actually sends anything, per the asyncio docs' own warning. Keep a strong
    # reference until each notification task finishes.
    background_tasks: set = set()

    def _remember_session(ctx: Context):
        last_session_holder["session"] = ctx.session

    def notify_mcp_host(method: str, params: Dict[str, Any]):
        session = last_session_holder["session"]
        if session is None:
            return
        try:
            task = asyncio.create_task(session.send_tool_list_changed())
            background_tasks.add(task)
            task.add_done_callback(background_tasks.discard)
        except Exception as e:
            logger.error(f"Failed to send tool_list_changed notification: {e}")

    protocol.on_inbound_message = notify_mcp_host

    @asynccontextmanager
    async def _lifespan(server: FastMCP):
        protocol.register_session_descriptor(session_name=protocol.session_name, kind="bg")
        listener_task = asyncio.create_task(protocol.start_bridge_listener())
        try:
            yield
        finally:
            listener_task.cancel()
            try:
                await listener_task
            except (asyncio.CancelledError, Exception):
                pass
            protocol.cleanup_session_descriptor()
            protocol.cleanup_socket()
            logger.info("Bridge stdio handler shut down gracefully.")

    mcp = FastMCP(name="claudemessaging", version="0.3.0", instructions=INSTRUCTIONS, lifespan=_lifespan)

    @mcp.tool()
    async def list_sessions(ctx: Context) -> str:
        """Scans workspace configurations, purges dead session files, and returns active Claude sessions."""
        _remember_session(ctx)
        peers = await protocol.verify_and_purge_sessions()
        return json.dumps(peers, indent=2)

    @mcp.tool()
    async def purge_sessions(ctx: Context) -> str:
        """Force scans and cleans up dead socket and session files."""
        _remember_session(ctx)
        peers = await protocol.verify_and_purge_sessions()
        return f"Purge scan completed. Active sessions remaining: {len(peers)}"

    @mcp.tool()
    async def rename_session(new_name: str, ctx: Context) -> str:
        """Renames this bridge's announced session descriptor in ~/.claude/sessions/ so Claude Code instances discover it under the new name via ListAgents."""
        _remember_session(ctx)
        old_name = protocol.session_name
        protocol.cleanup_session_descriptor()
        protocol.session_name = new_name
        protocol.register_session_descriptor(session_name=new_name)
        res = {
            "success": True,
            "previous_name": old_name,
            "new_name": new_name,
            "pid": getattr(protocol, "registered_pid", os.getpid()),
            "descriptor_file": getattr(protocol, "session_json_path", "")
        }
        return json.dumps(res, indent=2)

    @mcp.tool()
    async def send_message(session: str, message: str, ctx: Context) -> str:
        """Dispatches an authenticated user message to a target Claude Code session. Fire-and-forget: any real reply the target sends back arrives later as its own inbound activity, observable via get_responses - this does not wait for or return a reply."""
        _remember_session(ctx)
        result = await protocol.send_to_claude(session_identifier=session, message_content=message)
        if not result.get("success", False):
            raise ToolError(result.get("error", "send_message failed"))
        return json.dumps(result, indent=2)

    @mcp.tool()
    async def get_responses(ctx: Context, msg_id: Optional[str] = None) -> str:
        """Fetches inbound and outbound message response histories cached by the bridge."""
        _remember_session(ctx)
        resp = protocol.response_store.get(msg_id, {"error": f"Message ID '{msg_id}' not found."}) if msg_id else protocol.response_store
        return json.dumps(resp, indent=2)

    return mcp


def make_protocol(bridge_socket_path: Optional[str], session_name: Optional[str]) -> ClaudeMessagingProtocol:
    return ClaudeMessagingProtocol(bridge_socket_path=bridge_socket_path, session_name=session_name)


async def run_mcp_server(protocol: ClaudeMessagingProtocol):
    mcp = build_mcp_server(protocol)
    await mcp.run_stdio_async(show_banner=False)
