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
import logging
import traceback

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Initialize Firestore client
db = firestore.Client()

# Retrieve API_KEY from environment variables
API_KEY = os.environ.get('API_KEY')

@functions_framework.http
def AbuSegal_Express(request):
    # Check for API key
    if request.args.get('api_key') != API_KEY:
        logger.warning("Unauthorized access attempt.")
        return 'Unauthorized', 401

    # Access secrets from Secret Manager
    try:
        # Create a Secret Manager client
        secret_client = secretmanager.SecretManagerServiceClient()

        # Build the resource names of the secrets
        project_id = os.environ.get('GCP_PROJECT') or os.environ.get('GOOGLE_CLOUD_PROJECT')
        if not project_id:
            raise Exception('Project ID not found in environment variables.')

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
        traceback_str = ''.join(traceback.format_tb(e.__traceback__))
        logger.error(f"Error accessing secrets: {e}\nTraceback:\n{traceback_str}")
        return f'Error accessing secrets: {e}', 500

    # Define an asynchronous task to run the main logic
    async def main_async():
        client = None
        try:
            # Create a Telegram client
            client = TelegramClient(StringSession(session_string), api_id, api_hash)
            await client.start()
            logger.info("Telegram client started.")

            # Run fetch_and_store_posts for all channels concurrently
            tasks = [
                fetch_and_store_posts(channel_username, db, client)
                for channel_username in CHANNEL_USERNAMES
            ]
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"An error occurred during fetching/storing posts: {e}", exc_info=True)
            raise e
        finally:
            if client:
                await client.disconnect()
                logger.info("Telegram client disconnected.")

    # Run the asynchronous task within a new event loop
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(main_async())
    except Exception as e:
        traceback_str = ''.join(traceback.format_tb(e.__traceback__))
        logger.error(f"Error running main_async: {e}\nTraceback:\n{traceback_str}")
        return f"An error occurred: {e}", 500
    finally:
        loop.close()

    # Retrieve and format posts
    try:
        posts = get_combined_feed(db, israel_tz)
        html_content = format_as_html(posts)
    except Exception as e:
        traceback_str = ''.join(traceback.format_tb(e.__traceback__))
        logger.error(f"Error formatting posts: {e}\nTraceback:\n{traceback_str}")
        return f"Error formatting posts: {e}", 500

    # Return the HTML content
    return html_content, 200, {'Content-Type': 'text/html'}
