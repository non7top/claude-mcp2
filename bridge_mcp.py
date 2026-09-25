#!/usr/bin/env python3
import os
import sys
import json
import glob
import uuid
import stat
import time
import asyncio
import logging
import psutil
import signal
import argparse
from typing import Dict, Any, Optional

# MCP protocols communicate over stdout, logs MUST route to stderr
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stderr,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("SocketBridgeMCP")

class ClaudeMessageBridgeMCP:
    def __init__(
        self,
        bridge_socket_path: Optional[str] = None,
        session_name: Optional[str] = None,
        auto_reply: bool = True
    ):
        self.pid = os.getpid()
        self.home_dir = os.path.expanduser("~")
        self.sessions_dir = os.path.join(self.home_dir, ".claude", "sessions")
        self.auto_reply = auto_reply

        env_socket = os.environ.get("AGY_MCP_BRIDGE_SOCKET")
        if bridge_socket_path:
            self.bridge_socket_path = bridge_socket_path
        elif env_socket:
            self.bridge_socket_path = env_socket
        else:
            self.bridge_socket_path = self._get_default_socket_path(self.pid)

        env_name = os.environ.get("AGY_SESSION_NAME")
        self.session_name = session_name or env_name or "antigravity-bridge"

        # Historical datastore for storing response payloads
        self.response_store: Dict[str, Dict[str, Any]] = {}
        self.active_peers: Dict[str, Dict[str, Any]] = {}

    def _get_default_socket_path(self, pid: int) -> str:
        try:
            uid = os.getuid()
            cc_socks_dir = f"/run/user/{uid}/cc-socks"
            if os.path.exists(cc_socks_dir) and os.access(cc_socks_dir, os.W_OK):
                return os.path.join(cc_socks_dir, f"{pid}.sock")

            fallback_dir = "/tmp/cc-socks"
            os.makedirs(fallback_dir, exist_ok=True)
            return os.path.join(fallback_dir, f"{pid}.sock")
        except Exception:
            pass
        return f"/tmp/agy_mcp_bridge_{pid}.sock"

    def _is_pid_alive(self, pid: Optional[int]) -> bool:
        if not pid or not isinstance(pid, int):
            return False
        try:
            if psutil.pid_exists(pid):
                try:
                    os.kill(pid, 0)
                    return True
                except (OSError, ProcessLookupError):
                    return False
            return False
        except Exception:
            return False

    async def verify_and_purge_sessions(self) -> Dict[str, Dict[str, Any]]:
        """
        Scans workspace configurations, checks if tracking PIDs are alive,
        and systematically cleans up orphaned or dead sockets and key files.
        """
        logger.info("Running proactive session structural analysis...")
        if not os.path.exists(self.sessions_dir):
            logger.warning(f"Sessions directory does not exist: {self.sessions_dir}")
            self.active_peers = {}
            return {}

        current_sessions: Dict[str, Dict[str, Any]] = {}
        json_files = glob.glob(os.path.join(self.sessions_dir, "*.json"))
        purged_count = 0

        for file_path in json_files:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    meta = json.load(f)

                pid = meta.get("pid")
                sock_path = meta.get("messagingSocketPath")
                name = meta.get("name", f"session-{pid}")
                session_id = meta.get("sessionId", "")
                cwd = meta.get("cwd", "")

                # Check process lifecycle
                if pid and self._is_pid_alive(pid):
                    # Check for accompanying auth key file
                    key_pattern = os.path.join(self.sessions_dir, f"{pid}.*.key")
                    discovered_keys = glob.glob(key_pattern)

                    token = ""
                    if discovered_keys:
                        try:
                            with open(discovered_keys[0], "r", encoding="utf-8") as kf:
                                key_raw = kf.read().strip()
                                try:
                                    key_data = json.loads(key_raw)
                                    if isinstance(key_data, dict) and "peerToken" in key_data:
                                        token = key_data["peerToken"]
                                    else:
                                        token = key_raw
                                except json.JSONDecodeError:
                                    token = key_raw
                        except Exception as kerr:
                            logger.error(f"Failed to read key file {discovered_keys[0]}: {kerr}")

                    current_sessions[name] = {
                        "name": name,
                        "pid": pid,
                        "sessionId": session_id,
                        "socket": sock_path,
                        "token": token,
                        "cwd": cwd,
                        "updatedAt": meta.get("updatedAt"),
                        "status": meta.get("status", "active")
                    }
                else:
                    # Target process is dead; purge stale descriptor and key files safely
                    logger.warning(f"Purging dead session files for PID {pid}")
                    try:
                        os.remove(file_path)
                        purged_count += 1
                    except OSError as e:
                        logger.error(f"Failed to remove stale session file {file_path}: {e}")

                    # Also purge associated key files for dead PID
                    if pid:
                        key_pattern = os.path.join(self.sessions_dir, f"{pid}.*.key")
                        for key_file in glob.glob(key_pattern):
                            try:
                                os.remove(key_file)
                                logger.info(f"Purged stale key file: {key_file}")
                            except OSError as e:
                                logger.error(f"Failed to remove stale key file {key_file}: {e}")
            except Exception as e:
                logger.error(f"Error checking session file {file_path}: {e}")

        self.active_peers = current_sessions
        logger.info(f"Session analysis complete. Active peers: {len(current_sessions)}, Purged: {purged_count}")
        return self.active_peers

    def _resolve_session(self, session_identifier: str) -> Optional[Dict[str, Any]]:
        if not self.active_peers:
            return None

        # Direct match by name
        if session_identifier in self.active_peers:
            return self.active_peers[session_identifier]

        # Match by PID (str or int)
        for peer in self.active_peers.values():
            if str(peer.get("pid")) == str(session_identifier):
                return peer

        # Match by SessionID
        for peer in self.active_peers.values():
            if peer.get("sessionId") == session_identifier:
                return peer

        # Substring / case-insensitive match on name
        session_lower = session_identifier.lower()
        for name, peer in self.active_peers.items():
            if session_lower in name.lower():
                return peer

        # If user passed "default", "first", or "active", return first active session
        if session_identifier.lower() in ["default", "first", "active", "any"]:
            return next(iter(self.active_peers.values()))

        return None

    def _find_transcript_file(self, session_id: str, cwd: Optional[str] = None) -> Optional[str]:
        if not session_id:
            return None

        # 1. Direct glob match across project folders
        pattern = os.path.expanduser(f"~/.claude/projects/*/{session_id}.jsonl")
        matches = glob.glob(pattern)
        if matches:
            return matches[0]

        # 2. Check predicted path from cwd if provided
        if cwd:
            sanitized_cwd = cwd.replace("/", "-")
            predicted_path = os.path.expanduser(f"~/.claude/projects/{sanitized_cwd}/{session_id}.jsonl")
            if os.path.exists(predicted_path):
                return predicted_path

        # 3. Recursive glob search across all project directories
        rec_pattern = os.path.expanduser(f"~/.claude/projects/**/{session_id}.jsonl")
        rec_matches = glob.glob(rec_pattern, recursive=True)
        if rec_matches:
            return rec_matches[0]

        return None

    async def wait_for_assistant_response(
        self,
        session_id: str,
        start_offset: int,
        pid: Optional[int] = None,
        cwd: Optional[str] = None,
        timeout: float = 60.0
    ) -> Dict[str, Any]:
        """
        Monitors the target session transcript file until Claude finishes generating
        its response turn, then extracts and returns the assistant's response text.
        """
        start_time = asyncio.get_event_loop().time()
        transcript_file = self._find_transcript_file(session_id, cwd=cwd)

        max_wait_file = min(15.0, timeout)
        while not transcript_file:
            if asyncio.get_event_loop().time() - start_time > max_wait_file:
                break
            await asyncio.sleep(0.5)
            transcript_file = self._find_transcript_file(session_id, cwd=cwd)

        if not transcript_file or not os.path.exists(transcript_file):
            logger.warning(f"Transcript file for session {session_id} not found on disk.")
            return {"completed": False, "content": "", "error": f"Transcript file for session '{session_id}' not found on disk"}

        collected_texts = []
        turn_completed = False

        try:
            with open(transcript_file, "r", encoding="utf-8") as f:
                f.seek(start_offset)

                while asyncio.get_event_loop().time() - start_time < timeout:
                    line = f.readline()
                    if not line:
                        if pid and self.sessions_dir:
                            session_file = os.path.join(self.sessions_dir, f"{pid}.json")
                            if os.path.exists(session_file):
                                try:
                                    with open(session_file, "r", encoding="utf-8") as sf:
                                        sdata = json.load(sf)
                                        if sdata.get("status") == "idle" and collected_texts:
                                            turn_completed = True
                                            break
                                except Exception:
                                    pass
                        await asyncio.sleep(0.3)
                        continue

                    try:
                        data = json.loads(line.strip())
                        msg_type = data.get("type")

                        if msg_type == "assistant":
                            content = data.get("message", {}).get("content", [])
                            if isinstance(content, list):
                                for block in content:
                                    if isinstance(block, dict) and block.get("type") == "text":
                                        text_str = block.get("text", "").strip()
                                        if text_str:
                                            collected_texts.append(text_str)
                            elif isinstance(content, str):
                                text_str = content.strip()
                                if text_str:
                                    collected_texts.append(text_str)

                        elif msg_type == "system" and data.get("subtype") == "turn_duration":
                            turn_completed = True
                            break
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            logger.error(f"Error reading transcript file {transcript_file}: {e}")

        final_text = "\n\n".join(collected_texts).strip()
        return {
            "completed": turn_completed or bool(final_text),
            "content": final_text,
            "transcript_path": transcript_file
        }

    async def _background_monitor_response(
        self,
        msg_id: str,
        session_id: str,
        start_offset: int,
        pid: Optional[int],
        cwd: Optional[str],
        timeout: float
    ):
        resp_data = await self.wait_for_assistant_response(session_id, start_offset, pid, cwd, timeout)
        if msg_id in self.response_store:
            self.response_store[msg_id]["status"] = "completed" if resp_data.get("completed") else "timeout"
            self.response_store[msg_id]["response"] = resp_data.get("content", "")

    async def send_to_claude(
        self,
        session_identifier: str,
        message_content: str,
        wait_for_response: bool = True,
        timeout: float = 60.0
    ) -> Dict[str, Any]:
        """
        Establishes an out-bound wire pipe to a verified target session.
        Uses key assets to authenticate and bypass confirmation steps.
        If wait_for_response is True, waits for Claude to generate its turn and returns the response.
        """
        await self.verify_and_purge_sessions()

        peer = self._resolve_session(session_identifier)
        if not peer:
            logger.error(f"Target session '{session_identifier}' is not reachable.")
            return {
                "success": False,
                "error": f"Target session '{session_identifier}' not found among active sessions: {list(self.active_peers.keys())}"
            }

        socket_path = peer.get("socket")
        token = peer.get("token", "")
        session_id = peer.get("sessionId", "")
        pid = peer.get("pid")
        cwd = peer.get("cwd")

        if not socket_path or not os.path.exists(socket_path):
            logger.error(f"Socket path for '{peer['name']}' does not exist: {socket_path}")
            return {
                "success": False,
                "error": f"Socket path '{socket_path}' for session '{peer['name']}' does not exist on disk."
            }

        # Determine start offset in transcript before dispatching
        start_offset = 0
        tfile = self._find_transcript_file(session_id, cwd=cwd)
        if tfile and os.path.exists(tfile):
            try:
                start_offset = os.path.getsize(tfile)
            except OSError:
                start_offset = 0

        # Build NDJSON wire packets
        auth_frame = {"type": "auth", "peerToken": token}
        msg_id = f"msg_{uuid.uuid4()}"
        message_frame = {
            "type": "user",
            "message": {"role": "user", "content": message_content},
            "priority": "next",
            "msg_id": msg_id,
            "sender": self.session_name
        }

        try:
            reader, writer = await asyncio.open_unix_connection(socket_path)
            writer.write((json.dumps(auth_frame) + "\n").encode("utf-8"))
            writer.write((json.dumps(message_frame) + "\n").encode("utf-8"))
            await writer.drain()

            try:
                ack_line = await asyncio.wait_for(reader.readline(), timeout=2.0)
                if ack_line:
                    logger.info(f"Received instant ACK from {peer['name']}: {ack_line.decode('utf-8').strip()}")
            except (asyncio.TimeoutError, Exception):
                pass

            writer.close()
            await writer.wait_closed()

            entry = {
                "status": "dispatched",
                "msg_id": msg_id,
                "target_session": peer["name"],
                "target_pid": peer["pid"],
                "content": message_content,
                "response": ""
            }
            self.response_store[msg_id] = entry
            logger.info(f"Successfully dispatched message {msg_id} to {peer['name']}")

            if wait_for_response:
                logger.info(f"Waiting for response turn from {peer['name']} (timeout={timeout}s)...")
                resp_data = await self.wait_for_assistant_response(
                    session_id=session_id,
                    start_offset=start_offset,
                    pid=pid,
                    cwd=cwd,
                    timeout=timeout
                )
                response_text = resp_data.get("content", "")
                entry["status"] = "completed" if resp_data.get("completed") else "timeout"
                entry["response"] = response_text
                self.response_store[msg_id] = entry

                return {
                    "success": True,
                    "msg_id": msg_id,
                    "target": peer["name"],
                    "status": entry["status"],
                    "response": response_text,
                    "detail": entry
                }
            else:
                asyncio.create_task(
                    self._background_monitor_response(msg_id, session_id, start_offset, pid, cwd, timeout)
                )
                return {
                    "success": True,
                    "msg_id": msg_id,
                    "target": peer["name"],
                    "status": "dispatched",
                    "detail": entry
                }

        except Exception as e:
            logger.error(f"IPC injection crash sending to {socket_path}: {e}")
            return {
                "success": False,
                "error": f"Failed to send message over Unix socket to {socket_path}: {e}"
            }

    def record_inbound_turn(self, sender: str, content: str, reply_text: Optional[str] = None):
        """
        Records inbound message turn into the bridge transcript file
        so querying processes can read turns and responses seamlessly.
        """
        if not getattr(self, "bridge_transcript_path", None):
            return

        try:
            with open(self.bridge_transcript_path, "a", encoding="utf-8") as f:
                user_turn = {
                    "type": "user",
                    "message": {"role": "user", "content": content},
                    "sender": sender,
                    "timestamp": time.time()
                }
                f.write(json.dumps(user_turn) + "\n")

                if reply_text:
                    assistant_turn = {
                        "type": "assistant",
                        "message": {
                            "role": "assistant",
                            "content": [{"type": "text", "text": reply_text}]
                        },
                        "timestamp": time.time()
                    }
                    duration_turn = {
                        "type": "system",
                        "subtype": "turn_duration",
                        "durationMs": 100,
                        "timestamp": time.time()
                    }
                    f.write(json.dumps(assistant_turn) + "\n")
                    f.write(json.dumps(duration_turn) + "\n")
        except Exception as e:
            logger.error(f"Failed to append to bridge transcript file: {e}")

    def register_session_descriptor(self, session_name: str = "antigravity-bridge"):
        """
        Registers a session descriptor in ~/.claude/sessions/ so that surrounding
        Claude Code processes can discover and message this bridge via ListAgents.
        """
        if not os.path.exists(self.sessions_dir):
            try:
                os.makedirs(self.sessions_dir, exist_ok=True)
            except OSError as e:
                logger.error(f"Failed to create sessions directory: {e}")
                return

        self.registered_pid = os.getpid()
        self.registered_session_id = str(uuid.uuid4())
        self.registered_token = uuid.uuid4().hex
        now = int(time.time() * 1000)

        self.session_json_path = os.path.join(self.sessions_dir, f"{self.registered_pid}.json")
        self.session_key_path = os.path.join(self.sessions_dir, f"{self.registered_pid}.{self.registered_token[:16]}.key")

        # Create a dedicated bridge transcript directory and file
        sanitized_name = session_name.replace("/", "-")
        self.bridge_transcript_dir = os.path.expanduser(f"~/.claude/projects/-bridge-session-{sanitized_name}")
        os.makedirs(self.bridge_transcript_dir, exist_ok=True)
        self.bridge_transcript_path = os.path.join(self.bridge_transcript_dir, f"{self.registered_session_id}.jsonl")

        session_data = {
            "pid": self.registered_pid,
            "sessionId": self.registered_session_id,
            "cwd": os.getcwd(),
            "startedAt": now,
            "procStart": str(self.registered_pid),
            "version": "2.1.282",
            "peerProtocol": 1,
            "peerFeatures": ["notify_idle", "reply_across_default_dirs", "artifact_yield"],
            "kind": "interactive",
            "entrypoint": "cli",
            "pidDomain": f"linux:local:pid:[{self.registered_pid}]",
            "messagingSocketPath": self.bridge_socket_path,
            "name": session_name,
            "nameSource": "user",
            "nameSince": now,
            "status": "idle",
            "updatedAt": now,
            "statusUpdatedAt": now
        }

        key_data = {
            "peerToken": self.registered_token,
            "procStart": str(self.registered_pid)
        }

        try:
            with open(self.session_json_path, "w", encoding="utf-8") as f:
                json.dump(session_data, f, indent=2)
            with open(self.session_key_path, "w", encoding="utf-8") as f:
                json.dump(key_data, f, indent=2)
            logger.info(f"🚀 Registered bridge session descriptor '{session_name}' (PID {self.registered_pid}) in {self.sessions_dir}")
        except Exception as e:
            logger.error(f"Failed to register session descriptor: {e}")

    def cleanup_session_descriptor(self):
        """
        Cleans up the bridge's registered session descriptor, key files, and transcript files on shutdown.
        """
        for p in [getattr(self, "session_json_path", None), getattr(self, "session_key_path", None), getattr(self, "bridge_transcript_path", None)]:
            if p and os.path.exists(p):
                try:
                    os.remove(p)
                    logger.info(f"Cleaned up session file: {p}")
                except OSError as e:
                    logger.error(f"Failed to remove descriptor file {p}: {e}")

    def notify_mcp_host(self, method: str, params: Dict[str, Any]):
        """
        Emits an asynchronous MCP JSON-RPC notification line over stdout to notify the host (agy).
        """
        notification = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params
        }
        try:
            sys.stdout.write(json.dumps(notification) + "\n")
            sys.stdout.flush()
            logger.info(f"Poked MCP host with notification '{method}': {params.get('msg_id')}")
        except Exception as e:
            logger.error(f"Failed to write MCP notification to stdout: {e}")

    async def handle_inbound_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """
        Manages inbound response tracking. Listens for external peer confirmations
        and caches payloads for immediate lookup.
        """
        try:
            while not reader.at_eof():
                line = await reader.readline()
                if not line:
                    break
                line_str = line.decode("utf-8").strip()
                if not line_str:
                    continue

                try:
                    payload = json.loads(line_str)
                    frame_type = payload.get("type")

                    if frame_type == "auth":
                        logger.info("Inbound peer authenticated successfully.")
                        continue

                    msg_id = payload.get("msg_id", f"inbound_{uuid.uuid4()}")
                    sender = payload.get("sender", "unknown")

                    msg_obj = payload.get("message", {})
                    if isinstance(msg_obj, dict):
                        content_val = msg_obj.get("content", payload.get("content", ""))
                    else:
                        content_val = payload.get("content", str(msg_obj))

                    # Cache response
                    self.response_store[msg_id] = {
                        "status": payload.get("status", "received"),
                        "sender": sender,
                        "content": content_val,
                        "payload": payload,
                        "timestamp": time.time()
                    }
                    logger.info(f"Buffered inbound response frame: {msg_id}")

                    # Generate auto-reply turn if enabled
                    reply_text = f"Pong! Received: '{content_val}'" if self.auto_reply else None
                    self.record_inbound_turn(sender=sender, content=content_val, reply_text=reply_text)

                    # Notify agy host continuously in background over stdout
                    self.notify_mcp_host(
                        method="notifications/message",
                        params={
                            "msg_id": msg_id,
                            "sender": sender,
                            "content": content_val,
                            "timestamp": time.time()
                        }
                    )

                    ack = json.dumps({"status": "received", "msg_id": msg_id}) + "\n"
                    try:
                        writer.write(ack.encode("utf-8"))
                        await writer.drain()
                    except (ConnectionResetError, BrokenPipeError):
                        pass

                except json.JSONDecodeError:
                    logger.error("Received invalid JSON payload over inbound bridge socket")
        except (ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError):
            logger.info("Inbound peer connection closed.")
        except Exception as e:
            logger.error(f"Error handling inbound traffic: {e}")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def start_bridge_listener(self):
        """
        Binds an isolated socket interface to aggregate answers and announce
        the server's endpoint to surrounding environments.
        """
        if os.path.exists(self.bridge_socket_path):
            try:
                os.remove(self.bridge_socket_path)
            except OSError as e:
                logger.error(f"Failed to remove stale bridge socket {self.bridge_socket_path}: {e}")

        server = await asyncio.start_unix_server(
            self.handle_inbound_client, self.bridge_socket_path
        )
        # Apply secure single-user runtime read/write bounds
        os.chmod(self.bridge_socket_path, stat.S_IRUSR | stat.S_IWUSR)
        logger.info(f"🚀 Bridge server operational on: {self.bridge_socket_path}")

        async with server:
            await server.serve_forever()

    async def mcp_stdio_loop(self):
        """
        Standard JSON-RPC 2.0 loop reading from stdin and answering via stdout.
        Exposes core functions directly to the connected engine (Antigravity).
        """
        loop = asyncio.get_event_loop()
        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        await loop.connect_read_pipe(lambda: protocol, sys.stdin)

        def write_response(resp: dict):
            sys.stdout.write(json.dumps(resp) + "\n")
            sys.stdout.flush()

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
                write_response({
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {"code": -32700, "message": "Parse error"}
                })
                continue

            req_id = request.get("id")
            method = request.get("method")
            params = request.get("params", {})

            # Handle notifications (requests without id)
            if req_id is None:
                if method == "notifications/initialized":
                    logger.info("MCP client initialized notification received.")
                else:
                    logger.info(f"Received notification: {method}")
                continue

            # Standard MCP Methods
            if method == "initialize":
                write_response({
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "protocolVersion": "2024-11-05",
                        "capabilities": {
                            "tools": {}
                        },
                        "serverInfo": {
                            "name": "claudemessaging",
                            "version": "1.0.0"
                        },
                        "instructions": "Bi-Directional AI Communication MCP Bridge for Claude Code sessions."
                    }
                })
            elif method == "ping":
                write_response({
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {}
                })
            elif method == "tools/list":
                write_response({
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "tools": [
                            {
                                "name": "list_sessions",
                                "description": "Scans workspace configurations, purges dead session files, and returns active Claude sessions.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {},
                                    "required": []
                                }
                            },
                            {
                                "name": "send_message",
                                "description": "Sends an authenticated user message to a target Claude Code session and waits for its response.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "session": {
                                            "type": "string",
                                            "description": "Target session name, PID, or Session ID"
                                        },
                                        "message": {
                                            "type": "string",
                                            "description": "Text message content to deliver"
                                        },
                                        "wait": {
                                            "type": "boolean",
                                            "description": "Whether to wait for Claude to finish generating its response (default true)"
                                        },
                                        "timeout": {
                                            "type": "number",
                                            "description": "Maximum seconds to wait for response turn completion (default 60)"
                                        }
                                    },
                                    "required": ["session", "message"]
                                }
                            },
                            {
                                "name": "get_responses",
                                "description": "Fetches inbound and outbound message response histories cached by the bridge.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "msg_id": {
                                            "type": "string",
                                            "description": "Optional message ID filter"
                                        }
                                    },
                                    "required": []
                                }
                            },
                            {
                                "name": "purge_sessions",
                                "description": "Force scans and cleans up dead socket and session files.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {},
                                    "required": []
                                }
                            },
                            {
                                "name": "rename_session",
                                "description": "Renames this bridge's announced session descriptor in ~/.claude/sessions/ so Claude Code instances discover it under the new name via ListAgents.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "new_name": {
                                            "type": "string",
                                            "description": "The new session name to announce (e.g. 'antigravity-dev-bridge')"
                                        }
                                    },
                                    "required": ["new_name"]
                                }
                            }
                        ]
                    }
                })
            elif method == "tools/call":
                tool_name = params.get("name")
                args = params.get("arguments", {})

                try:
                    if tool_name == "list_sessions":
                        peers = await self.verify_and_purge_sessions()
                        write_response({
                            "jsonrpc": "2.0",
                            "id": req_id,
                            "result": {
                                "content": [
                                    {"type": "text", "text": json.dumps(peers, indent=2)}
                                ],
                                "isError": False
                            }
                        })
                    elif tool_name == "purge_sessions":
                        peers = await self.verify_and_purge_sessions()
                        write_response({
                            "jsonrpc": "2.0",
                            "id": req_id,
                            "result": {
                                "content": [
                                    {"type": "text", "text": f"Purge scan completed. Active sessions remaining: {len(peers)}"}
                                ],
                                "isError": False
                            }
                        })
                    elif tool_name == "rename_session":
                        new_name = args.get("new_name")
                        if not new_name:
                            write_response({
                                "jsonrpc": "2.0",
                                "id": req_id,
                                "result": {
                                    "content": [
                                        {"type": "text", "text": "Error: 'new_name' argument is required."}
                                    ],
                                    "isError": True
                                }
                            })
                        else:
                            old_name = getattr(self, "session_name", "antigravity-bridge")
                            self.cleanup_session_descriptor()
                            self.session_name = new_name
                            self.register_session_descriptor(session_name=new_name)

                            res = {
                                "success": True,
                                "previous_name": old_name,
                                "new_name": new_name,
                                "pid": getattr(self, "registered_pid", os.getpid()),
                                "descriptor_file": getattr(self, "session_json_path", "")
                            }
                            write_response({
                                "jsonrpc": "2.0",
                                "id": req_id,
                                "result": {
                                    "content": [
                                        {"type": "text", "text": json.dumps(res, indent=2)}
                                    ],
                                    "isError": False
                                }
                            })
                    elif tool_name == "send_message":
                        target_session = args.get("session")
                        message_text = args.get("message")
                        should_wait = args.get("wait", True)
                        timeout_val = float(args.get("timeout", 60.0))

                        if not target_session or not message_text:
                            write_response({
                                "jsonrpc": "2.0",
                                "id": req_id,
                                "result": {
                                    "content": [
                                        {"type": "text", "text": "Error: Both 'session' and 'message' arguments are required."}
                                    ],
                                    "isError": True
                                }
                            })
                        else:
                            result = await self.send_to_claude(
                                session_identifier=target_session,
                                message_content=message_text,
                                wait_for_response=should_wait,
                                timeout=timeout_val
                            )
                            is_err = not result.get("success", False)
                            write_response({
                                "jsonrpc": "2.0",
                                "id": req_id,
                                "result": {
                                    "content": [
                                        {"type": "text", "text": json.dumps(result, indent=2)}
                                    ],
                                    "isError": is_err
                                }
                            })
                    elif tool_name == "get_responses":
                        msg_id = args.get("msg_id")
                        if msg_id:
                            resp = self.response_store.get(msg_id, {"error": f"Message ID '{msg_id}' not found."})
                        else:
                            resp = self.response_store
                        write_response({
                            "jsonrpc": "2.0",
                            "id": req_id,
                            "result": {
                                "content": [
                                    {"type": "text", "text": json.dumps(resp, indent=2)}
                                ],
                                "isError": False
                            }
                        })
                    else:
                        write_response({
                            "jsonrpc": "2.0",
                            "id": req_id,
                            "error": {"code": -32601, "message": f"Tool '{tool_name}' not found."}
                        })
                except Exception as e:
                    logger.error(f"Error handling tool call '{tool_name}': {e}")
                    write_response({
                        "jsonrpc": "2.0",
                        "id": req_id,
                        "result": {
                            "content": [
                                {"type": "text", "text": f"Error executing tool '{tool_name}': {str(e)}"}
                            ],
                            "isError": True
                        }
                    })
            else:
                write_response({
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": {"code": -32601, "message": f"Method '{method}' not found."}
                })

    def cleanup_socket(self):
        if os.path.exists(self.bridge_socket_path):
            try:
                os.remove(self.bridge_socket_path)
                logger.info(f"Cleaned up socket file {self.bridge_socket_path}")
            except OSError as e:
                logger.error(f"Error removing socket file {self.bridge_socket_path}: {e}")

    async def run_standalone(self):
        """
        Runs the bridge as a standalone daemon server without the stdio MCP loop.
        Registers session descriptor and listens on Unix domain socket for inbound messages.
        """
        self.register_session_descriptor(session_name=self.session_name)

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

        listener_task = asyncio.create_task(self.start_bridge_listener())
        stop_task = asyncio.create_task(stop_event.wait())

        done, pending = await asyncio.wait(
            [listener_task, stop_task],
            return_when=asyncio.FIRST_COMPLETED
        )

        for task in pending:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

        self.cleanup_session_descriptor()
        self.cleanup_socket()
        logger.info("Bridge standalone daemon shut down gracefully.")

    async def run_all(self):
        # Register session descriptor so Claude Code instances discover us via ListAgents
        self.register_session_descriptor(session_name=self.session_name)

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

        listener_task = asyncio.create_task(self.start_bridge_listener())
        stdio_task = asyncio.create_task(self.mcp_stdio_loop())
        stop_task = asyncio.create_task(stop_event.wait())

        done, pending = await asyncio.wait(
            [listener_task, stdio_task, stop_task],
            return_when=asyncio.FIRST_COMPLETED
        )

        for task in pending:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

        self.cleanup_session_descriptor()
        self.cleanup_socket()
        logger.info("Bridge daemon shut down gracefully.")

def main():
    parser = argparse.ArgumentParser(description="Claude Message Bridge MCP Server & Standalone CLI Tool")
    parser.add_argument("--socket", type=str, default=None, help="Path to bridge Unix socket")
    parser.add_argument("--name", type=str, default="antigravity-bridge", help="Session name announced to Claude Code")

    mode_group = parser.add_argument_group("Execution Modes")
    mode_group.add_argument("--standalone", "--daemon", action="store_true", help="Run as standalone daemon server without stdio MCP loop")
    mode_group.add_argument("--list", action="store_true", help="List all active Claude Code sessions")
    mode_group.add_argument("--send", nargs=2, metavar=("SESSION", "MESSAGE"), help="Send message to target Claude Code session")
    mode_group.add_argument("--purge", action="store_true", help="Proactively scan and purge dead session files")

    send_group = parser.add_argument_group("Send & Server Options")
    send_group.add_argument("--no-wait", action="store_true", help="Do not wait for assistant response turn when sending message")
    send_group.add_argument("--timeout", type=float, default=60.0, help="Timeout in seconds for response turn completion (default: 60)")
    send_group.add_argument("--auto-reply", action="store_true", default=None, help="Enable automatic pong response turns for inbound messages (default: true for standalone mode)")
    send_group.add_argument("--no-auto-reply", action="store_false", dest="auto_reply", help="Disable automatic pong response turns")

    args = parser.parse_args()

    # Default auto_reply to True for all bridge instances, unless explicitly disabled
    auto_reply_setting = True if args.auto_reply is None else args.auto_reply

    bridge = ClaudeMessageBridgeMCP(
        bridge_socket_path=args.socket,
        session_name=args.name,
        auto_reply=auto_reply_setting
    )

    if args.list:
        async def _cmd_list():
            peers = await bridge.verify_and_purge_sessions()
            print(json.dumps(peers, indent=2))
        asyncio.run(_cmd_list())
        sys.exit(0)

    elif args.purge:
        async def _cmd_purge():
            peers = await bridge.verify_and_purge_sessions()
            print(f"Purge scan completed. Active sessions remaining: {len(peers)}")
        asyncio.run(_cmd_purge())
        sys.exit(0)

    elif args.send:
        target_session, message_text = args.send[0], args.send[1]
        should_wait = not args.no_wait
        timeout_val = args.timeout

        async def _cmd_send():
            res = await bridge.send_to_claude(
                session_identifier=target_session,
                message_content=message_text,
                wait_for_response=should_wait,
                timeout=timeout_val
            )
            print(json.dumps(res, indent=2))
            if not res.get("success"):
                sys.exit(1)
        asyncio.run(_cmd_send())
        sys.exit(0)

    elif args.standalone:
        try:
            asyncio.run(bridge.run_standalone())
        except (KeyboardInterrupt, SystemExit):
            logger.info("Shutting down standalone bridge daemon gracefully.")
        except Exception as e:
            logger.error(f"Fatal standalone server error: {e}")
            sys.exit(0)

    else:
        try:
            asyncio.run(bridge.run_all())
        except (KeyboardInterrupt, SystemExit):
            logger.info("Shutting down bridge daemon gracefully.")
        except Exception as e:
            logger.error(f"Fatal server error: {e}")
            sys.exit(0)

if __name__ == "__main__":
    main()
