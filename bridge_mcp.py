#!/usr/bin/env python3
"""
Thin compatibility shim for direct invocation (`python3 bridge_mcp.py ...`)
without installing the package first. The real entry point is claude_bridge.cli.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from claude_bridge.cli import main

if __name__ == "__main__":
    main()
