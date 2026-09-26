FROM python:3.14-slim

ARG UID=1000
ARG GID=1000

RUN groupadd -g ${GID} appuser \
    && useradd -m -u ${UID} -g ${GID} -d /home/appuser appuser

WORKDIR /app

# Install dependencies first (pyproject.toml + the claude_chatter package) so
# the pip layer is cached independently of test_suite.py edits.
COPY pyproject.toml bridge_mcp.py ./
COPY claude_chatter/ ./claude_chatter/
RUN pip install --no-cache-dir . pytest

COPY test_suite.py ./

# Give this container its OWN $HOME (with its own ~/.claude/sessions), entirely
# separate from the host's. claude_chatter reads/writes ~/.claude/sessions for
# real session discovery - tests must never be able to see or mutate a
# developer's actual live sessions.
RUN mkdir -p /home/appuser/.claude/sessions /home/appuser/.claude/projects \
    && chown -R appuser:appuser /home/appuser /app

USER appuser
ENV HOME=/home/appuser

CMD ["python", "-m", "pytest", "test_suite.py", "-v"]
