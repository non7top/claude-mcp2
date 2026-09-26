"""
MCP stdio entry point. Thin adapter over ClaudeMessagingProtocol: owns the
JSON-RPC/stdio transport and tool routing, delegates everything else to the
protocol layer. Tied 1:1 to this stdio connection's lifetime - when the MCP
host (Antigravity) restarts the connection, the fresh process just
re-registers under the same cwd-derived name immediately.
"""
import os
import sys
import json
import asyncio
import logging
import signal
from typing import Any, Dict, Optional

from .protocol import ClaudeMessagingProtocol

logger = logging.getLogger("SocketBridgeMCP")

TOOLS = [
    {
        "name": "list_sessions",
        "description": "Scans workspace configurations, purges dead session files, and returns active Claude sessions.",
        "inputSchema": {"type": "object", "properties": {}, "required": []}
    },
    {
        "name": "send_message",
        "description": "Dispatches an authenticated user message to a target Claude Code session. Fire-and-forget: any real reply the target sends back arrives later as its own inbound activity, observable via get_responses - this does not wait for or return a reply.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "session": {"type": "string", "description": "Target session name, PID, or Session ID"},
                "message": {"type": "string", "description": "Text message content to deliver"}
            },
            "required": ["session", "message"]
        }
    },
    {
        "name": "get_responses",
        "description": "Fetches inbound and outbound message response histories cached by the bridge.",
        "inputSchema": {
            "type": "object",
            "properties": {"msg_id": {"type": "string", "description": "Optional message ID filter"}},
            "required": []
        }
    },
    {
        "name": "purge_sessions",
        "description": "Force scans and cleans up dead socket and session files.",
        "inputSchema": {"type": "object", "properties": {}, "required": []}
    },
    {
        "name": "rename_session",
        "description": "Renames this bridge's announced session descriptor in ~/.claude/sessions/ so Claude Code instances discover it under the new name via ListAgents.",
        "inputSchema": {
            "type": "object",
            "properties": {"new_name": {"type": "string", "description": "The new session name to announce (e.g. 'antigravity-dev-bridge')"}},
            "required": ["new_name"]
        }
    }
]


def notify_mcp_host(method: str, params: Dict[str, Any]):
    """Emits an asynchronous MCP JSON-RPC notification line over stdout to notify the host (agy)."""
    if sys.stdout.isatty():
        return
    notification = {"jsonrpc": "2.0", "method": method, "params": params}
    try:
        sys.stdout.write(json.dumps(notification) + "\n")
        sys.stdout.flush()
        logger.info(f"Poked MCP host with notification '{method}': {params.get('msg_id')}")
    except (BrokenPipeError, OSError):
        logger.info("MCP host stdout pipe closed.")
    except Exception as e:
        logger.error(f"Failed to write MCP notification to stdout: {e}")


async def _tool_call(protocol: ClaudeMessagingProtocol, tool_name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    """Executes one MCP tool call against the protocol layer and returns an MCP result payload."""
    if tool_name == "list_sessions":
        peers = await protocol.verify_and_purge_sessions()
        return {"content": [{"type": "text", "text": json.dumps(peers, indent=2)}], "isError": False}

    if tool_name == "purge_sessions":
        peers = await protocol.verify_and_purge_sessions()
        return {"content": [{"type": "text", "text": f"Purge scan completed. Active sessions remaining: {len(peers)}"}], "isError": False}

    if tool_name == "rename_session":
        new_name = args.get("new_name")
        if not new_name:
            return {"content": [{"type": "text", "text": "Error: 'new_name' argument is required."}], "isError": True}
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
        return {"content": [{"type": "text", "text": json.dumps(res, indent=2)}], "isError": False}

    if tool_name == "send_message":
        target_session = args.get("session")
        message_text = args.get("message")
        if not target_session or not message_text:
            return {"content": [{"type": "text", "text": "Error: Both 'session' and 'message' arguments are required."}], "isError": True}
        result = await protocol.send_to_claude(
            session_identifier=target_session,
            message_content=message_text
        )
        return {"content": [{"type": "text", "text": json.dumps(result, indent=2)}], "isError": not result.get("success", False)}

    if tool_name == "get_responses":
        msg_id = args.get("msg_id")
        resp = protocol.response_store.get(msg_id, {"error": f"Message ID '{msg_id}' not found."}) if msg_id else protocol.response_store
        return {"content": [{"type": "text", "text": json.dumps(resp, indent=2)}], "isError": False}

    return None


async def mcp_stdio_loop(protocol: ClaudeMessagingProtocol):
    """Standard JSON-RPC 2.0 loop reading from stdin and answering via stdout."""
    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader()
    reader_protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: reader_protocol, sys.stdin)

    def write_response(resp: dict):
        try:
            sys.stdout.write(json.dumps(resp) + "\n")
            sys.stdout.flush()
        except (BrokenPipeError, OSError):
            logger.warning("MCP host stdout pipe closed; cannot write response.")

    while True:
        line = await reader.readline()
        if not line:
            break

        line_str = line.decode("utf-8").strip()
        if not line_str:
            continue

        try:
            request = json.loads(line_str)
        except json.JSONDecodeError as e:
            logger.error(f"JSON-RPC parse error: {e}")
            write_response({"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"}})
            continue

        req_id = request.get("id")
        method = request.get("method")
        params = request.get("params", {})

        if req_id is None:
            if method == "notifications/initialized":
                logger.info("MCP client initialized notification received.")
            else:
                logger.info(f"Received notification: {method}")
            continue

        if method == "initialize":
            write_response({
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "claudemessaging", "version": "1.0.0"},
                    "instructions": "Bi-Directional AI Communication MCP Bridge for Claude Code sessions."
                }
            })
        elif method == "ping":
            write_response({"jsonrpc": "2.0", "id": req_id, "result": {}})
        elif method == "tools/list":
            write_response({"jsonrpc": "2.0", "id": req_id, "result": {"tools": TOOLS}})
        elif method == "tools/call":
            tool_name = params.get("name")
            args = params.get("arguments", {})
            try:
                result = await _tool_call(protocol, tool_name, args)
                if result is None:
                    write_response({"jsonrpc": "2.0", "id": req_id, "error": {"code": -32601, "message": f"Tool '{tool_name}' not found."}})
                else:
                    write_response({"jsonrpc": "2.0", "id": req_id, "result": result})
            except Exception as e:
                logger.error(f"Error handling tool call '{tool_name}': {e}")
                write_response({
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {"content": [{"type": "text", "text": f"Error executing tool '{tool_name}': {str(e)}"}], "isError": True}
                })
        else:
            write_response({"jsonrpc": "2.0", "id": req_id, "error": {"code": -32601, "message": f"Method '{method}' not found."}})


async def run_mcp_server(protocol: ClaudeMessagingProtocol):
    protocol.register_session_descriptor(session_name=protocol.session_name, kind="bg")

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _handle_signal():
        logger.info("Termination signal received. Initiating graceful shutdown...")
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except (NotImplementedError, RuntimeError):
            pass

    listener_task = asyncio.create_task(protocol.start_bridge_listener())
    stdio_task = asyncio.create_task(mcp_stdio_loop(protocol))
    stop_task = asyncio.create_task(stop_event.wait())

    # Only the stdio channel itself (or an explicit stop signal) should end this
    # process - a listener hiccup must not tear down an otherwise-healthy
    # connection to the MCP host.
    done, pending = await asyncio.wait([stdio_task, stop_task], return_when=asyncio.FIRST_COMPLETED)

    remaining_tasks = set(pending)
    remaining_tasks.add(listener_task)
    for task in remaining_tasks:
        task.cancel()
    for task in remaining_tasks:
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass

    protocol.cleanup_session_descriptor()
    protocol.cleanup_socket()
    logger.info("Bridge stdio handler shut down gracefully.")


def make_protocol(bridge_socket_path: Optional[str], session_name: Optional[str]) -> ClaudeMessagingProtocol:
    return ClaudeMessagingProtocol(
        bridge_socket_path=bridge_socket_path,
        session_name=session_name,
        on_inbound_message=notify_mcp_host
    )
