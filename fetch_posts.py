from datetime import datetime, timedelta
from telethon.errors import ChannelPrivateError, ChannelInvalidError
from telethon.tl.types import (
    MessageMediaPhoto,
    MessageMediaDocument,
    DocumentAttributeFilename
)
from google.cloud import firestore, storage
from utils import israel_tz
import google.api_core.exceptions
import asyncio
from io import BytesIO
import logging
import traceback

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Initialize the Storage Client
storage_client = storage.Client()
bucket_name = 'abu_segal_images'  # Replace with your actual bucket name
bucket = storage_client.bucket(bucket_name)

# Firestore batch size limit
FIRESTORE_BATCH_LIMIT = 500

async def fetch_and_store_posts(channel_username, db, client):
    try:
        # Ensure the client is connected
        if not client.is_connected():
            await client.connect()
            logger.info("Client connected.")

        # Get the channel entity
        channel = await client.get_entity(channel_username)
        logger.info(f"Fetched entity for channel: {channel_username}")
    except (ValueError, ChannelPrivateError, ChannelInvalidError) as e:
        logger.error(f"Error accessing channel {channel_username}: {e}")
        return

    # Calculate time 24 hours ago
    yesterday = datetime.now(israel_tz) - timedelta(days=1)

    # Reference to the 'posts' collection
    posts_ref = db.collection('posts')

    # Query to get existing posts from the last 24 hours for this channel
    # Updated to use keyword arguments for filters to avoid warnings
    query = posts_ref.where('channel_id', '==', channel_username).where('date', '>', yesterday)
    existing_posts = query.stream()

    # Collect existing message IDs
    existing_post_ids = set()
    for doc in existing_posts:
        existing_post_ids.add(doc.to_dict().get('id'))

    # Lists to hold posts to store and document references to delete
    posts_to_store = []
    posts_to_delete = []

    # Fetch messages from the last 24 hours
    async for message in client.iter_messages(channel):
        message_date = message.date.astimezone(israel_tz)

        # Stop the loop if the message is older than 24 hours
        if message_date < yesterday:
            break

        message_id = str(message.id)

        # Skip if the message is already in the database
        if message_id in existing_post_ids:
            continue  # Skip this message as it's already in the db

        text = message.message or ''
        image_urls = []

        # Collect images from the message
        media_list = []

        if message.grouped_id:
            # Fetch messages in the group (album)
            grouped_messages = []
            async for msg in client.iter_messages(channel, offset_id=message.id - 9, max_id=message.id):
                if msg.grouped_id == message.grouped_id:
                    grouped_messages.append(msg)
                elif msg.id < message.id - 9:
                    break  # No need to go further back
            # Sort messages by their IDs
            grouped_messages.sort(key=lambda x: x.id)
            for msg in grouped_messages:
                if msg.media:
                    media_list.append(msg)
        else:
            if message.media:
                media_list.append(message)

        # Process each media item in the message or group
        for media_msg in media_list:
            if isinstance(media_msg.media, MessageMediaPhoto):
                logger.info(f"Message {media_msg.id} contains a photo.")
                try:
                    # Create an in-memory bytes buffer
                    buffer = BytesIO()
                    # Use client.download_media instead of media_msg.download_media
                    await client.download_media(media_msg, file=buffer)
                    buffer.seek(0)  # Move to the beginning of the buffer

                    # Generate a unique filename for Cloud Storage
                    blob_name = f"{channel_username}/{media_msg.id}.jpg"

                    # Create a blob in your bucket
                    blob = bucket.blob(blob_name)

                    # Upload the image from the buffer
                    await asyncio.to_thread(blob.upload_from_file, buffer, content_type='image/jpeg')
                    logger.info(f"Uploaded photo to Cloud Storage: {blob_name}")

                    # Get the public URL
                    image_url = f"https://storage.googleapis.com/{bucket_name}/{blob_name}"
                    image_urls.append(image_url)
                    logger.info(f"Image URL: {image_url}")

                except Exception as e:
                    logger.error(f"Error processing photo for message {media_msg.id}: {e}", exc_info=True)
            elif isinstance(media_msg.media, MessageMediaDocument):
                # Check if the document is an image
                if media_msg.document.mime_type.startswith('image/'):
                    logger.info(f"Message {media_msg.id} contains an image document.")
                    try:
                        # Create an in-memory bytes buffer
                        buffer = BytesIO()
                        await client.download_media(media_msg, file=buffer)
                        buffer.seek(0)  # Move to the beginning of the buffer

                        # Use the original filename if available
                        filename_attr = next(
                            (attr for attr in media_msg.document.attributes if isinstance(attr, DocumentAttributeFilename)),
                            None
                        )
                        filename = filename_attr.file_name if filename_attr else f"{media_msg.id}.jpg"

                        # Generate a unique filename for Cloud Storage
                        blob_name = f"{channel_username}/{filename}"

                        # Create a blob in your bucket
                        blob = bucket.blob(blob_name)

                        # Upload the image from the buffer
                        await asyncio.to_thread(blob.upload_from_file, buffer, content_type=media_msg.document.mime_type)
                        logger.info(f"Uploaded image document to Cloud Storage: {blob_name}")

                        # Get the public URL
                        image_url = f"https://storage.googleapis.com/{bucket_name}/{blob_name}"
                        image_urls.append(image_url)
                        logger.info(f"Image URL: {image_url}")

                    except Exception as e:
                        logger.error(f"Error processing image document for message {media_msg.id}: {e}", exc_info=True)
                else:
                    logger.info(f"Message {media_msg.id} contains a non-image document. Skipping.")
            else:
                logger.info(f"Message {media_msg.id} contains unsupported media type. Skipping.")

        post = {
            'id': message_id,
            'channel_id': channel_username,
            'text': text,
            'date': message_date,
            'images': image_urls
        }

        # Add the post to the list to store
        posts_to_store.append(post)
        logger.info(f"Prepared post for storage: {post}")

    # Prepare batch writes for storing posts
    if posts_to_store:
        await batch_write_posts(db, posts_to_store)

    # Prepare batch deletions for old posts
    await batch_delete_old_posts(channel_username, db)

    # Delete old media files from Cloud Storage
    await batch_delete_old_media(channel_username, bucket)

    # Ensure the client remains connected until all operations are complete
    # Removed client disconnect to maintain singleton

async def batch_write_posts(db, posts_to_store):
    """
    Writes multiple posts to Firestore in batches to reduce the number of write operations.
    """
    try:
        # Split the posts into batches of FIRESTORE_BATCH_LIMIT
        for i in range(0, len(posts_to_store), FIRESTORE_BATCH_LIMIT):
            batch = db.batch()
            batch_posts = posts_to_store[i:i + FIRESTORE_BATCH_LIMIT]
            for post in batch_posts:
                post_ref = db.collection('posts').document(f"{post['channel_id']}_{post['id']}")
                batch.set(post_ref, post)
            # Run the batch write in a separate thread to avoid blocking
            await asyncio.to_thread(batch.commit)
            logger.info(f"Committed a batch of {len(batch_posts)} posts to Firestore.")
    except Exception as e:
        logger.error(f"Error during batch write to Firestore: {e}", exc_info=True)
        raise e

async def batch_delete_old_posts(channel_username, db):
    """
    Deletes old posts from Firestore in batches.
    """
    try:
        # Calculate time 24 hours ago
        yesterday = datetime.now(israel_tz) - timedelta(days=1)

        # Reference to the 'posts' collection
        posts_ref = db.collection('posts')

        # Query to get posts older than 24 hours for this channel
        old_posts_query = posts_ref.where('channel_id', '==', channel_username).where('date', '<', yesterday)
        old_posts = old_posts_query.stream()

        # Collect document references to delete
        docs_to_delete = [doc.reference for doc in old_posts]

        if docs_to_delete:
            # Split into batches of FIRESTORE_BATCH_LIMIT
            for i in range(0, len(docs_to_delete), FIRESTORE_BATCH_LIMIT):
                batch = db.batch()
                batch_deletes = docs_to_delete[i:i + FIRESTORE_BATCH_LIMIT]
                for doc_ref in batch_deletes:
                    batch.delete(doc_ref)
                # Run the batch delete in a separate thread to avoid blocking
                await asyncio.to_thread(batch.commit)
                logger.info(f"Deleted a batch of {len(batch_deletes)} old posts from Firestore.")
    except Exception as e:
        logger.error(f"Error during batch deletion of old posts: {e}", exc_info=True)
        raise e


async def batch_delete_old_media(channel_username, bucket):
    """
    Deletes old media files from Cloud Storage in batches.
    """
    try:
        # Calculate time 24 hours ago
        yesterday = datetime.now(israel_tz) - timedelta(days=1)

        # List blobs in the channel's folder
        blobs = list(bucket.list_blobs(prefix=f"{channel_username}/"))

        # Collect blobs to delete
        blobs_to_delete = [blob for blob in blobs if blob.time_created.astimezone(israel_tz) < yesterday]

        if blobs_to_delete:
            # Cloud Storage's batch API allows multiple deletions
            # Use `delete()` in batches to optimize
            for i in range(0, len(blobs_to_delete), FIRESTORE_BATCH_LIMIT):
                batch_blobs = blobs_to_delete[i:i + FIRESTORE_BATCH_LIMIT]
                await asyncio.to_thread(lambda: [blob.delete() for blob in batch_blobs])
                logger.info(f"Deleted a batch of {len(batch_blobs)} media files from Cloud Storage.")
    except Exception as e:
        logger.error(f"Error during batch deletion of old media: {e}", exc_info=True)
        raise e

