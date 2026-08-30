#!/usr/bin/env python3
"""
Main entry point for the clash_scraper application.
Runs both the poller and server in a single process (non-forking).
"""

import sys
import time
import threading
import signal
import logging

# Import your modules
import poller
import server

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global flag for graceful shutdown
running = True

def signal_handler(sig, frame):
    """Handle shutdown signals gracefully"""
    global running
    logger.info("Received shutdown signal, exiting...")
    running = False

def run_poller():
    """Run the poller in a thread"""
    try:
        poller.main()
    except Exception as e:
        logger.error(f"Poller crashed: {e}")
        global running
        running = False

def run_server():
    """Run the server in a thread"""
    try:
        server.start()
    except Exception as e:
        logger.error(f"Server crashed: {e}")
        global running
        running = False

if __name__ == "__main__":
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logger.info("Starting clash_scraper...")
    
    # Start poller in a daemon thread (not a separate process)
    poller_thread = threading.Thread(target=run_poller, daemon=True)
    poller_thread.start()
    logger.info("✅ Poller thread started")
    
    # Run server in the main thread
    logger.info("✅ Starting server...")
    run_server()
    
    # Graceful shutdown
    logger.info("Application exiting")