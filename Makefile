DOCKER_UID := $(shell id -u)
DOCKER_GID := $(shell id -g)
export DOCKER_UID
export DOCKER_GID

.PHONY: build test lint run stop destroy

build:
	docker compose build

test: build
	docker compose run --rm test

lint: build
	docker compose run --rm test python -m py_compile bridge_mcp.py test_suite.py

# There is no containerized `run`: bridge_mcp.py's whole job is to read/write the
# HOST's real ~/.claude/sessions and its real Unix sockets under
# /run/user/$(UID)/cc-socks to bridge real Claude Code sessions. A container gets
# its own isolated $HOME (on purpose, so tests can never touch real session
# state) - running the bridge itself in one would cut it off from everything it's
# meant to talk to. It's installed and run host-native via uvx/uv tool install.
run:
	@echo "bridge_mcp.py runs host-native (uvx/uv tool install) - it needs the real ~/.claude/sessions and cc-socks. Not containerized; see comment in Makefile."

stop:
	docker compose stop

destroy:
	docker compose down -v --rmi local
