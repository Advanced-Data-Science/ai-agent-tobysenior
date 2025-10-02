"""
Spotify Data Collection Agent
Collects data from Spotify API using Client Credentials flow
Implements adaptive strategy, respectful rate limiting, and data quality assessment
"""

import os
import base64
import json
import time
import random
import logging
import requests
from dotenv import load_dotenv
from datetime import datetime

class SpotifyDataAgent:
    def __init__(self, config_file=".env"):
        """
        Initialize the agent
        Loads credentials and config from .env
        Sets up logging, data storage, and adaptive parameters
        """
        load_dotenv(config_file)
        self.client_id = os.getenv("CLIENT_ID")
        self.client_secret = os.getenv("CLIENT_SECRET")
        self.api_base = "https://api.spotify.com/v1"
        
        # Logging setup
        self.setup_logging()
        
        # Data store and statistics
        self.data_store = []
        self.collection_stats = {
            'start_time': datetime.now(),
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'quality_scores': []
        }
        self.delay_multiplier = 1.0

        # Configurable request parameters
        self.base_delay = float(os.getenv("BASE_DELAY", 1.0))
        self.max_requests = int(os.getenv("MAX_REQUESTS", 20))

    # ---------------- Logging ----------------
    def setup_logging(self):
        """Setup logging to console and file"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('spotify_data_collection.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    # ---------------- Spotify Token ----------------
    def get_token(self):
        """Get an access token from Spotify using Client Credentials flow"""
        auth_string = f"{self.client_id}:{self.client_secret}"
        auth_bytes = auth_string.encode("utf-8")
        auth_base64 = base64.b64encode(auth_bytes).decode("utf-8")

        url = "https://accounts.spotify.com/api/token"
        headers = {
            "Authorization": "Basic " + auth_base64,
            "Content-Type": "application/x-www-form-urlencoded"
        }
        data = {"grant_type": "client_credentials"}
        try:
            response = requests.post(url, headers=headers, data=data)
            response.raise_for_status()
            token = response.json()["access_token"]
            return token
        except Exception as e:
            self.logger.error(f"Failed to get token: {e}")
            return None

    # ---------------- Main Collection ----------------
    def run_collection(self):
        """Main loop to collect Spotify data until max requests reached"""
        self.logger.info("Starting Spotify Data Collection Agent")
        token = self.get_token()
        if not token:
            self.logger.error("No access token, stopping agent")
            return
        
        headers = {"Authorization": f"Bearer {token}"}

        while not self.collection_complete():
            data = self.collect_batch(headers)
            if data:
                processed = self.process_data(data)
                if self.validate_data(processed):
                    self.store_data(processed)
            self.assess_performance()
            self.respectful_delay()

        self.generate_final_report()

    # ---------------- Data Collection ----------------
    def collect_batch(self, headers):
        """
        Collect a batch of Spotify data
        Example: fetch new releases (can adapt to playlists, tracks, etc.)
        """
        self.collection_stats['total_requests'] += 1
        url = f"{self.api_base}/browse/new-releases?limit=10"

        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                self.collection_stats['successful_requests'] += 1
                return response.json()
            else:
                self.collection_stats['failed_requests'] += 1
                self.logger.warning(f"Spotify API returned status {response.status_code}")
                return None
        except Exception as e:
            self.collection_stats['failed_requests'] += 1
            self.logger.error(f"Spotify API request failed: {e}")
            return None

    # ---------------- Data Processing ----------------
    def process_data(self, data):
        """Process raw Spotify response into structured dict"""
        albums = data.get("albums", {}).get("items", [])
        return [{"name": a["name"], "artist": a["artists"][0]["name"], "id": a["id"]} for a in albums]

    def validate_data(self, data):
        """Check if each item has required fields"""
        return all("name" in d and "artist" in d for d in data)

    def store_data(self, data):
        """Store processed and validated data"""
        self.data_store.extend(data)

    # ---------------- Adaptive Strategy ----------------
    def assess_performance(self):
        """Monitor success rate and data quality; adjust strategy if needed"""
        success_rate = self.get_success_rate()
        quality_score = self.assess_data_quality()
        if success_rate < 0.8:
            self.adjust_strategy()
        self.logger.info(f"Success rate: {success_rate:.2f}, Quality score: {quality_score:.2f}")

    def get_success_rate(self):
        """Compute ratio of successful requests"""
        total = self.collection_stats['total_requests']
        successful = self.collection_stats['successful_requests']
        return (successful / total) if total > 0 else 0

    def assess_data_quality(self):
        """Evaluate completeness of collected data"""
        if not self.data_store:
            return 0
        complete_items = sum(1 for d in self.data_store if "name" in d and "artist" in d)
        score = complete_items / len(self.data_store)
        self.collection_stats['quality_scores'].append(score)
        return score

    def adjust_strategy(self):
        """Adapt delay based on performance"""
        success_rate = self.get_success_rate()
        if success_rate < 0.5:
            self.delay_multiplier *= 2
            self.logger.info("Low success rate: increasing delay")
        elif success_rate > 0.9:
            self.delay_multiplier *= 0.8
            self.logger.info("High success rate: decreasing delay")

    # ---------------- Respectful Rate Limiting ----------------
    def respectful_delay(self):
        """Implement respectful delay with jitter"""
        jitter = random.uniform(0.5, 1.5)
        delay = self.base_delay * self.delay_multiplier * jitter
        self.logger.debug(f"Sleeping for {delay:.2f}s")
        time.sleep(delay)

    # ---------------- Completion & Reporting ----------------
    def collection_complete(self):
        """Stop after max_requests"""
        return self.collection_stats['total_requests'] >= self.max_requests

    def generate_final_report(self):
        """Print final stats and sample collected data"""
        self.logger.info(f"Collection complete. Total requests: {self.collection_stats['total_requests']}")
        self.logger.info(f"Successful: {self.collection_stats['successful_requests']}, Failed: {self.collection_stats['failed_requests']}")
        self.logger.info(f"Collected items: {len(self.data_store)}")
        self.logger.info(f"Sample data: {self.data_store[:5]}")

# ---------------- Example Usage ----------------
if __name__ == "__main__":
    agent = SpotifyDataAgent()
    agent.run_collection()
