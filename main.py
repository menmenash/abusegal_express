import functions_framework
from google.cloud import firestore
from google.cloud import secretmanager
from telethon import TelegramClient
from telethon.sessions import StringSession
import os
import asyncio
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

    # Access secrets from Secret Manager
    try:
        # Create a Secret Manager client
        secret_client = secretmanager.SecretManagerServiceClient()

        # Build the resource names of the secrets
        project_id = os.environ.get('GCP_PROJECT') or os.environ.get('GOOGLE_CLOUD_PROJECT')
        if not project_id:
            return 'Project ID not found in environment variables.', 500

        api_id_secret_name = f"projects/{project_id}/secrets/TELEGRAM_API_ID/versions/latest"
        api_hash_secret_name = f"projects/{project_id}/secrets/TELEGRAM_API_HASH/versions/latest"
        session_secret_name = f"projects/{project_id}/secrets/TELEGRAM_STRING_SESSION/versions/latest"

        # Access the secrets
        api_id_response = secret_client.access_secret_version(name=api_id_secret_name)
        api_hash_response = secret_client.access_secret_version(name=api_hash_secret_name)
        session_response = secret_client.access_secret_version(name=session_secret_name)

        # Extract the secret payloads
        api_id = api_id_response.payload.data.decode('UTF-8')
        api_hash = api_hash_response.payload.data.decode('UTF-8')
        session_string = session_response.payload.data.decode('UTF-8')

    except Exception as e:
        import traceback
        traceback_str = ''.join(traceback.format_tb(e.__traceback__))
        print(f"Error accessing secrets: {e}\nTraceback:\n{traceback_str}")
        return f'Error accessing secrets: {e}', 500

    # Create a new event loop for this thread
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def main_async():
        client = None  # Initialize client to None
        try:
            # Create a single Telegram client instance using StringSession
            client = TelegramClient(StringSession(session_string), api_id, api_hash)
            await client.start()

            # Run the asynchronous function
            tasks = [
                fetch_and_store_posts(channel_username, db, client)
                for channel_username in CHANNEL_USERNAMES
            ]
            await asyncio.gather(*tasks)
        except Exception as e:
            print(f"An error occurred: {e}")
            import traceback
            traceback.print_exc()
        finally:
            if client:
                await client.disconnect()

    try:
        loop.run_until_complete(main_async())
    finally:
        loop.close()

    # Retrieve and format posts
    posts = get_combined_feed(db, israel_tz)
    html_content = format_as_html(posts)

    return html_content, 200, {'Content-Type': 'text/html'}
