import os
import base64
import json
import time
from dotenv import load_dotenv
import random
import logging
from datetime import datetime
from requests import post

# -------------------------
# 1. Spotify Data Collection Agent
# -------------------------
class SpotifyDataAgent:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.data_store = []
        self.collection_stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'quality_scores': [],
            'apis_used': set()
        }
        self.delay_multiplier = 1.0
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[logging.FileHandler('data_collection.log'),
                      logging.StreamHandler()]
        )
        self.logger = logging.getLogger(__name__)

    def get_token(self):
        auth_string = f"{self.client_id}:{self.client_secret}"
        auth_base64 = base64.b64encode(auth_string.encode()).decode()
        url = "https://accounts.spotify.com/api/token"
        headers = {
            "Authorization": f"Basic {auth_base64}",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        data = {"grant_type": "client_credentials"}
        result = post(url, headers=headers, data=data)
        self.collection_stats['apis_used'].add("Spotify API /token")
        return result.json()["access_token"]

    def collect_data(self, n=20):
        token = self.get_token()
        self.logger.info("Starting Spotify Data Collection Agent")
        for i in range(n):
            # Simulated data collection (replace with real API calls)
            item = {
                "name": f"Song {i}",
                "artist": f"Artist {i}",
                "id": f"id_{i}"
            }
            self.data_store.append(item)
            self.collection_stats['total_requests'] += 1
            self.collection_stats['successful_requests'] += 1
            self.collection_stats['quality_scores'].append(1.0)
            self.logger.info(f"Success rate: 1.0, Quality score: 1.0")
            # Respectful delay
            time.sleep(random.uniform(0.1, 0.3))
        self.logger.info(f"Collection complete. Total requests: {self.collection_stats['total_requests']}")
        self.logger.info(f"Successful: {self.collection_stats['successful_requests']}, Failed: {self.collection_stats['failed_requests']}")
        self.logger.info(f"Collected items: {len(self.data_store)}")
        self.logger.info(f"Sample data: {self.data_store[:5]}")

# -------------------------
# 2. Documentation & QA Functions
# -------------------------
def generate_metadata(data_store, collection_stats):
    metadata = {
        'collection_info': {
            'collection_date': datetime.now().isoformat(),
            'agent_version': '1.0',
            'collector': 'Toby Spaner',
            'total_records': len(data_store)
        },
        'data_sources': list(collection_stats.get('apis_used', [])),  # <-- convert set to list
        'quality_metrics': {
            'average_quality_score': sum(collection_stats['quality_scores']) / max(len(collection_stats['quality_scores']), 1)
        },
        'processing_history': {
            **collection_stats,
            'apis_used': list(collection_stats.get('apis_used', []))  # <-- convert set to list here too
        },
        'variables': {
            "name": "Song name",
            "artist": "Primary artist",
            "id": "Spotify unique identifier"
        }
    }
    with open("dataset_metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)
    print("Metadata generated: dataset_metadata.json")

def generate_quality_report(data_store, collection_stats):
    total = len(data_store)
    complete = sum(1 for d in data_store if "name" in d and "artist" in d)
    report = {
        'summary': {
            'total_records': total,
            'collection_success_rate': collection_stats['successful_requests'] / max(collection_stats['total_requests'], 1),
            'overall_quality_score': complete / max(total, 1)
        },
        'completeness_analysis': {
            'total_records': total,
            'complete_records': complete,
            'completeness_ratio': complete / max(total, 1)
        },
        'data_distribution': {d['artist']: sum(1 for x in data_store if x['artist'] == d['artist']) for d in data_store},
        'anomaly_detection': [d for d in data_store if "name" not in d or "artist" not in d],
        'recommendations': ["Increase collection frequency", "Monitor failed requests"]
    }
    with open("quality_report.json", "w") as f:
        json.dump(report, f, indent=2)
    print("Quality report generated: quality_report.json")

def generate_collection_summary(data_store, collection_stats):
    summary = {
        "total_data_points": len(data_store),
        "success_requests": collection_stats['successful_requests'],
        "failed_requests": collection_stats['failed_requests'],
        "quality_metrics": {
            "average_quality_score": sum(collection_stats['quality_scores']) / max(len(collection_stats['quality_scores']), 1)
        },
        "issues_encountered": [d for d in data_store if "name" not in d or "artist" not in d],
        "recommendations": ["Increase collection frequency", "Add more metadata fields", "Monitor API success trends"]
    }
    with open("collection_summary.json", "w") as f:
        json.dump(summary, f, indent=2)
    print("Collection summary generated: collection_summary.json")

# -------------------------
# 3. Main Execution
# -------------------------
if __name__ == "__main__":
    # Load .env first
    load_dotenv()

    # Load client ID/secret from environment variables
    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")

    if not client_id or not client_secret:
        raise ValueError("CLIENT_ID and CLIENT_SECRET must be set in environment variables")

    # Create agent and collect data
    agent = SpotifyDataAgent(client_id, client_secret)
    agent.collect_data(n=20)  # collect 20 items

    # Generate all reports immediately
    generate_metadata(agent.data_store, agent.collection_stats)
    generate_quality_report(agent.data_store, agent.collection_stats)
    generate_collection_summary(agent.data_store, agent.collection_stats)

