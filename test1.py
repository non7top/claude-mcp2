import os
import sys
import glob
import json
import socket
import uuid

def get_active_sessions():
    """Scans the Claude sessions directory and returns a list of active sessions."""
    home_dir = os.path.expanduser("~")
    sessions_dir = os.path.join(home_dir, ".claude", "sessions")

    if not os.path.exists(sessions_dir):
        print(f"❌ Claude sessions directory not found at: {sessions_dir}")
        print("Make sure Claude Code is currently running in an active terminal.")
        sys.exit(1)

    session_files = glob.glob(os.path.join(sessions_dir, "*.json"))
    sessions = []

    for file_path in session_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                # Ensure it has a valid socket path before listing
                if data.get("messagingSocketPath"):
                    sessions.append(data)
        except (json.JSONDecodeError, IOError):
            continue

    return sessions

def send_message_to_socket(socket_path, text):
    """Delivers an unauthenticated peer message payload over the Unix domain socket."""
    auth_frame = {"type": "auth", "peerToken": ""}
    message_frame = {
        "type": "user",
        "message": {
            "role": "user",
            "content": text
        },
        "priority": "next",
        "msg_id": f"msg_{uuid.uuid4()}"
    }

    try:
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client.connect(socket_path)
        client.sendall((json.dumps(auth_frame) + "\n").encode("utf-8"))
        client.sendall((json.dumps(message_frame) + "\n").encode("utf-8"))
        client.close()
        return True
    except Exception as e:
        print(f"❌ Failed to deliver message: {e}")
        return False

def main():
    print("🔄 Scanning for active Claude Code sessions...")
    sessions = get_active_sessions()

    if not sessions:
        print("❌ No active Claude Code sessions found. Open Claude Code first!")
        sys.exit(0)

    # 1. Show all available sessions
    print("\nSelect an active Claude Code session to bridge with:")
    for idx, session in enumerate(sessions, 1):
        name = session.get("name", "Unnamed Session")
        pid = session.get("pid", "Unknown PID")
        cwd = session.get("cwd", "Unknown Directory")
        print(f" [{idx}] \033[1m{name}\033[0m (PID: {pid})")
        print(f"     Folder: {cwd}")

    # 2. Let the user pick a session
    while True:
        try:
            choice = input("\nEnter session number: ").strip()
            choice_idx = int(choice) - 1
            if 0 <= choice_idx < len(sessions):
                target_session = sessions[choice_idx]
                break
            print("Invalid selection. Try again.")
        except ValueError:
            print("Please enter a valid number.")

    target_socket = target_session["messagingSocketPath"]
    target_name = target_session.get("name", "Selected Session")

    print(f"\n✅ Connected to \033[1m{target_name}\033[0m bridge!")
    print("Type your message below and press Enter to push it to Claude Code.")
    print("Type 'exit' or 'quit' to close this connection loop.\n")

    # 3. Interactive typing loop
    while True:
        try:
            user_input = input("\033[34mYou (Bridge) > \033[0m").strip()
            if not user_input:
                continue
            if user_input.lower() in ["exit", "quit"]:
                print("👋 Closing bridge session.")
                break

            success = send_message_to_socket(target_socket, user_input)
            if success:
                print("🚀 Message pushed to Claude terminal window successfully.\n")
        except (KeyboardInterrupt, EOFError):
            print("\n👋 Closing bridge session.")
            break

if __name__ == "__main__":
    main()
