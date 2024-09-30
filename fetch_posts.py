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

logger = logging.getLogger(__name__)

# Initialize the Storage Client
storage_client = storage.Client()
bucket_name = 'abu_segal_images' 
bucket = storage_client.bucket(bucket_name)

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
    query = posts_ref.where(
        filter=firestore.FieldFilter('channel_id', '==', channel_username)
    ).where(
        filter=firestore.FieldFilter('date', '>', yesterday)
    )
    existing_posts = query.stream()

    # Collect existing message IDs
    existing_post_ids = set()
    for doc in existing_posts:
        existing_post_ids.add(doc.to_dict().get('id'))

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
                    blob.upload_from_file(buffer, content_type='image/jpeg')
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
                        blob.upload_from_file(buffer, content_type=media_msg.document.mime_type)
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

        # Log the post data
        logger.info(f"Storing post: {post}")

        # Store the new post in the database with retry logic
        post_ref = db.collection('posts').document(f"{channel_username}_{message_id}")
        await store_post_with_retry(db, post_ref, post)

    # Delete old posts from Firestore
    await delete_old_posts(channel_username, db)

    # Delete old media files from Cloud Storage
    delete_old_media(channel_username, bucket)

    # Ensure the client remains connected until all operations are complete
    if client.is_connected():
        await client.disconnect()
        logger.info("Client disconnected.")

async def store_post_with_retry(db, post_ref, post_data, retries=5, delay=0.5):
    for attempt in range(retries):
        try:
            post_ref.set(post_data)
            return  # Success
        except google.api_core.exceptions.Aborted as e:
            logger.warning(f"Write conflict encountered, retrying... (attempt {attempt+1}/{retries})")
            await asyncio.sleep(delay)
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise e  # Re-raise the exception for non-retryable errors
    raise google.api_core.exceptions.Aborted("Failed to write post after multiple retries.")

async def delete_old_posts(channel_username, db):
    # Calculate time 24 hours ago
    yesterday = datetime.now(israel_tz) - timedelta(days=1)

    # Reference to the 'posts' collection
    posts_ref = db.collection('posts')

    # Query to get posts older than 24 hours for this channel
    old_posts_query = posts_ref.where(
        filter=firestore.FieldFilter('channel_id', '==', channel_username)
    ).where(
        filter=firestore.FieldFilter('date', '<', yesterday)
    )

    # Delete old posts
    old_posts = old_posts_query.stream()
    for doc in old_posts:
        logger.info(f"Deleting old post: {doc.id}")
        doc.reference.delete()

def delete_old_media(channel_username, bucket):
    # Calculate time 24 hours ago
    yesterday = datetime.now(israel_tz) - timedelta(days=1)

    # List blobs in the channel's folder
    blobs = bucket.list_blobs(prefix=f"{channel_username}/")

    for blob in blobs:
        # Get the blob's time_created in the timezone
        blob_time = blob.time_created.astimezone(israel_tz)

        if blob_time < yesterday:
            logger.info(f"Deleting old media file: {blob.name}")
            blob.delete()