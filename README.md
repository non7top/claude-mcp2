# Claude Message Bridge MCP Server (`claude-mcp2`)

A Model Context Protocol (MCP) execution bridge built for **Google Antigravity** and **Claude Code** to enable non-blocking, authenticated bi-directional AI communication.

## 🚀 Key Features

* **Session Discovery & Automatic Purging**: Scans local workspace configurations (`~/.claude/sessions/*.json`), verifies process lifecycles via PID tracking (`psutil`), and automatically purges orphaned session descriptors and stale `.key` credential files.
* **Authenticated Unix Socket IPC**: Authenticates outbound message frames directly into active target session sockets (`/run/user/<uid>/cc-socks/*.sock`) using exfiltrated `peerToken` credentials.
* **Synchronous & Asynchronous Response Extraction**: Automatically tracks session transcript logs (`~/.claude/projects/*/<sessionId>.jsonl`) to wait for and extract full assistant turn responses.
* **Dedicated Inbound Unix Socket Listener**: Listens on `/tmp/agy_mcp_bridge.sock` (with secure single-user `0600` permissions) for real-time peer confirmations and inbound message buffering.
* **Graceful Lifecycle & Signal Handling**: Catches `SIGTERM` and `SIGINT` signals to cleanly unbind sockets and exit with status code `0`, avoiding process termination errors during MCP server reloads.

---

## 🛠️ Architecture

```
┌──────────────────────┐             ┌──────────────────────┐
│  Google Antigravity  │             │     Claude Code      │
│     (Prose / agy)    │             │   (Execution / CLI)  │
└──────────┬───────────┘             └──────────▲───────────┘
           │                                    │
    (MCP Tool Calls)                 (Pushed NDJSON Stream)
           │                                    │
┌──────────▼────────────────────────────────────┴───────────┐
│                 SOCKET BRIDGE MCP SERVER                  │
│  ┌───────────────────────┐    ┌─────────────────────────┐ │
│  │ Response History Cache│    │  Active Session Monitor │ │
│  │     (In-Memory)       │    │  (Dead Socket Purging)  │ │
│  └───────────────────────┘    └─────────────────────────┘ │
│  ┌───────────────────────┐    ┌─────────────────────────┐ │
│  │   Inbound IPC Server  │    │ Outbound Socket Manager │ │
│  │   Listener Daemon     │    │   (Key-Authenticated)   │ │
│  └───────────────────────┘    └─────────────────────────┘ │
└───────────────────────────────────────────────────────────┘
```

---

## 📦 Installation & Setup

### Option 1: Via `uvx` or `uv tool run` (Zero-Installation / Ephemeral)

Register directly with **Antigravity CLI (`agy`)** from GitHub without cloning:

**Using `uvx`:**
```bash
agy mcp add claudemessaging uvx --from git+https://github.com/non7top/claude-mcp2.git bridge-mcp
```

**Using `uv tool run`:**
```bash
agy mcp add claudemessaging uv tool run --from git+https://github.com/non7top/claude-mcp2.git bridge-mcp
```

Or run directly in terminal:
```bash
uvx --from git+https://github.com/non7top/claude-mcp2.git bridge-mcp
```

---

### Option 2: Permanent Tool Installation (`uv tool install`)

Install the `bridge-mcp` binary permanently into your system path using `uv`:

```bash
# 1. Install tool permanently into ~/.local/bin (or uv tool path)
uv tool install git+https://github.com/non7top/claude-mcp2.git

# 2. Register with Antigravity CLI
agy mcp add claudemessaging bridge-mcp
```

---

### Option 3: Local Repository Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/non7top/claude-mcp2.git
   cd claude-mcp2
   ```

2. **Register with Antigravity CLI (`agy`) using `uv` / `uvx`**:
   ```bash
   agy mcp add claudemessaging uvx --from . bridge-mcp
   ```

3. **Or register with standard `python3`**:
   ```bash
   agy mcp add claudemessaging python3 $(pwd)/bridge_mcp.py
   ```

4. **Verify registration**:
   ```bash
   agy mcp list
   ```

---

## 🧰 MCP Tool Reference

The MCP server exposes four primary tools:

### 1. `list_sessions`
Scans Claude Code workspace configurations, cleans up orphaned/dead session files, and returns all active Claude sessions.
* **Arguments**: None

### 2. `send_message`
Sends an authenticated user message frame to a target Claude Code session and waits for its response turn completion.
* **Arguments**:
  * `session` (string, required): Target session name (e.g. `rgle`), PID, or Session ID.
  * `message` (string, required): Text message content to deliver.
  * `wait` (boolean, optional, default: `true`): Whether to wait for Claude to generate its response turn.
  * `timeout` (number, optional, default: `60.0`): Maximum seconds to wait for turn completion.

### 3. `get_responses`
Queries historical inbound and outbound message response records cached in memory.
* **Arguments**:
  * `msg_id` (string, optional): Specific message ID filter.

### 4. `purge_sessions`
Manually triggers proactive scanning and deletion of dead socket descriptors and key files.
* **Arguments**: None

### 5. `rename_session`
Renames this bridge's announced session descriptor in `~/.claude/sessions/` so surrounding Claude Code processes discover it under the new name via `ListAgents`.
* **Arguments**:
  * `new_name` (string, required): The new session name to announce (e.g. `antigravity-dev-bridge`).

---

## 🖥️ Standalone Daemon & CLI Mode

In addition to standard stdio MCP mode, `bridge_mcp.py` can be launched in **standalone mode** (as a persistent background daemon or command-line execution tool):

### Standalone Daemon Server
Run the bridge as a background service without stdio MCP binding:
```bash
bridge-mcp --standalone --name custom-bridge-name
```
Or with `python3`:
```bash
python3 bridge_mcp.py --standalone --name custom-bridge-name
```

### Command-Line Execution Tools
Execute single-shot operations directly from terminal:

* **List Active Sessions**:
  ```bash
  bridge-mcp --list
  ```
* **Proactively Purge Dead Sessions**:
  ```bash
  bridge-mcp --purge
  ```
* **Send Message to Session**:
  ```bash
  bridge-mcp --send rgle "Hello from CLI"
  ```
* **Send Asynchronously (`--no-wait`)**:
  ```bash
  bridge-mcp --send rgle "Long running background task" --no-wait
  ```

---

## ⚙️ Running Tests

Run the included unit and integration test suites:

```bash
python3 test_suite.py
```

---

## 📄 License

MIT License. See [LICENSE](LICENSE) for details.
