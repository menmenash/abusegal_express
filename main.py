import functions_framework
from google.cloud import firestore
import os
from fetch_posts import fetch_and_store_posts
from format_posts import get_combined_feed, format_as_html
from utils import CHANNEL_USERNAMES, israel_tz

# Initialize Firestore client
db = firestore.Client()

API_KEY = os.environ.get('API_KEY')

@functions_framework.http
def AbuSegal_Express(request):
    # Check for API key
    if request.args.get('api_key') != API_KEY:
        return 'Unauthorized', 401

    # Fetch and store new posts
    for channel_username in CHANNEL_USERNAMES:
        fetch_and_store_posts(channel_username, db)
    
    # Retrieve and format posts
    posts = get_combined_feed(db, israel_tz)
    html_content = format_as_html(posts)
    
    return html_content, 200, {'Content-Type': 'text/html'}