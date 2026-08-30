"""
Web server to keep the Render service alive.
"""
from flask import Flask, jsonify
import subprocess
import threading
import os
import sys
import logging

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.route('/')
def home():
    return jsonify({
        "status": "running",
        "service": "Clash League Fixture Scraper",
        "endpoints": {
            "/scrape/leagues": "Trigger the multi-league scraper (?league=epl|seriea|ucl|europa|facup|community_shield|all, default all)",
            "/scrape/epl-round": "Fetch only one round of EPL fixtures (?round=N to pin a round, default auto-detects next round)",
            "/health": "Health check"
        },
        "note": "The old World Cup-only /scrape endpoint was removed -- leagues_scraper.py "
                "(via poller.py's automatic rolling 7-day window) is now the sole scrape path. "
                "Use /scrape/leagues?league=all for a manual full-catalog run."
    })

@app.route('/health')
def health():
    return jsonify({"status": "healthy"})


@app.route('/scrape/leagues')
def trigger_leagues_scraper():
    """Run leagues_scraper.py in the background for one or all leagues."""
    from flask import request

    league = request.args.get('league', 'all')

    def run_leagues_scraper():
        try:
            logger.info(f"📋 Leagues scraper triggered via /scrape/leagues?league={league}")
            process = subprocess.Popen(
                [sys.executable, 'leagues_scraper.py', '--league', league],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )
            for line in process.stdout:
                logger.info(f"📤 {line.strip()}")
            process.wait()
            logger.info(f"✅ Leagues scraper finished with code: {process.returncode}")
        except Exception as e:
            logger.error(f"❌ Leagues scraper failed: {e}")

    thread = threading.Thread(target=run_leagues_scraper)
    thread.start()
    return jsonify({
        "status": "started",
        "message": f"Leagues scraper triggered in background for league={league}",
        "endpoint": "/scrape/leagues"
    })


@app.route('/scrape/epl-round')
def trigger_epl_round_scraper():
    """Run leagues_scraper.py --league epl --round-only in the background."""
    from flask import request

    round_num = request.args.get('round')
    cmd = [sys.executable, 'leagues_scraper.py', '--league', 'epl', '--round-only']
    if round_num:
        cmd += ['--round-num', str(round_num)]

    def run_epl_round_scraper():
        try:
            logger.info(f"📋 EPL round scraper triggered via /scrape/epl-round (round={round_num or 'auto'})")
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )
            for line in process.stdout:
                logger.info(f"📤 {line.strip()}")
            process.wait()
            logger.info(f"✅ EPL round scraper finished with code: {process.returncode}")
        except Exception as e:
            logger.error(f"❌ EPL round scraper failed: {e}")

    thread = threading.Thread(target=run_epl_round_scraper)
    thread.start()
    return jsonify({
        "status": "started",
        "message": f"EPL round scraper triggered in background (round={round_num or 'auto'})",
        "endpoint": "/scrape/epl-round"
    })

def start():
    """Entrypoint used by main.py when running poller+server together."""
    port = int(os.environ.get('PORT', 10000))
    logger.info(f"🚀 Starting server on port {port}")
    app.run(host='0.0.0.0', port=port)

if __name__ == '__main__':
    start()