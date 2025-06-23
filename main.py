#!/usr/bin/env python3
"""
Cosmic Agent Framework - Main Entry Point
Run with: uv run
"""

import cosmicagentframework
from cosmicagentframework.cli import main

if __name__ == '__main__':
    # Initialize the framework
    cosmicagentframework.init()
    
    # Run the CLIW
    main()
