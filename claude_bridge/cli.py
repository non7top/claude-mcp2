"""
CLI entry point: argument parsing and mode dispatch. Thin - all real behavior
lives in protocol.py / mcp_server.py / standalone.py.
"""
import sys
import json
import asyncio
import logging
import argparse

from .protocol import ClaudeMessagingProtocol
from .standalone import run_standalone
from .mcp_server import run_mcp_server, make_protocol

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stderr,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("SocketBridgeMCP")


def main():
    parser = argparse.ArgumentParser(description="Claude Bridge MCP Server & Standalone CLI Tool")
    parser.add_argument("--socket", type=str, default=None, help="Path to bridge Unix socket")
    parser.add_argument("--name", type=str, default=None, help="Session name announced to Claude Code (default: derived from cwd, stable across restarts)")

    mode_group = parser.add_argument_group("Execution Modes")
    mode_group.add_argument("--standalone", "--daemon", action="store_true", help="Run as standalone daemon server without stdio MCP loop")
    mode_group.add_argument("--list", action="store_true", help="List all active Claude Code sessions")
    mode_group.add_argument("--send", nargs=2, metavar=("SESSION", "MESSAGE"), help="Send message to target Claude Code session")
    mode_group.add_argument("--purge", action="store_true", help="Proactively scan and purge dead session files")

    args = parser.parse_args()

    if args.list:
        protocol = ClaudeMessagingProtocol(bridge_socket_path=args.socket, session_name=args.name)

        async def _cmd_list():
            peers = await protocol.verify_and_purge_sessions()
            print(json.dumps(peers, indent=2))
        asyncio.run(_cmd_list())
        sys.exit(0)

    elif args.purge:
        protocol = ClaudeMessagingProtocol(bridge_socket_path=args.socket, session_name=args.name)

        async def _cmd_purge():
            peers = await protocol.verify_and_purge_sessions()
            print(f"Purge scan completed. Active sessions remaining: {len(peers)}")
        asyncio.run(_cmd_purge())
        sys.exit(0)

    elif args.send:
        protocol = ClaudeMessagingProtocol(bridge_socket_path=args.socket, session_name=args.name)
        target_session, message_text = args.send[0], args.send[1]

        async def _cmd_send():
            res = await protocol.send_to_claude(
                session_identifier=target_session,
                message_content=message_text
            )
            print(json.dumps(res, indent=2))
            if not res.get("success"):
                sys.exit(1)
        asyncio.run(_cmd_send())
        sys.exit(0)

    elif args.standalone:
        protocol = ClaudeMessagingProtocol(bridge_socket_path=args.socket, session_name=args.name)
        try:
            asyncio.run(run_standalone(protocol))
        except (KeyboardInterrupt, SystemExit):
            logger.info("Shutting down standalone bridge daemon gracefully.")
        except Exception as e:
            logger.error(f"Fatal standalone server error: {e}")
            sys.exit(0)

    else:
        protocol = make_protocol(args.socket, args.name)
        try:
            asyncio.run(run_mcp_server(protocol))
        except (KeyboardInterrupt, SystemExit):
            logger.info("Shutting down bridge daemon gracefully.")
        except Exception as e:
            logger.error(f"Fatal server error: {e}")
            sys.exit(0)


if __name__ == "__main__":
    main()
