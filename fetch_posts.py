import requests
from bs4 import BeautifulSoup
from datetime import datetime
from utils import israel_tz

def fetch_and_store_posts(channel_username, db):
    url = f'https://t.me/s/{channel_username}'
    response = requests.get(url)
    response.encoding = 'utf-8'
    soup = BeautifulSoup(response.text, 'html.parser')
    
    for message in soup.find_all('div', class_='tgme_widget_message'):
        message_id = message['data-post'].split('/')[-1]

        time_element = message.find('time', class_='time')
        if time_element and 'datetime' in time_element.attrs:
            date = time_element['datetime']
            utc_time = datetime.fromisoformat(date.replace('Z', '+00:00'))
            israel_time = utc_time.astimezone(israel_tz)
        else:
            israel_time = None
            print(f"Warning: No time element found for message {message_id}.")

        text_element = message.find('div', class_='tgme_widget_message_text')
        text = text_element.get_text() if text_element else ''

        image_elements = message.find_all('a', class_='tgme_widget_message_photo_wrap')
        image_urls = [img['style'].split("url('")[1].split("')")[0] for img in image_elements if 'style' in img.attrs]

        post = {
            'id': message_id,
            'channel_id': f'@{channel_username}',
            'text': text,
            'date': israel_time,
            'images': image_urls
        }
        db.collection('posts').document(f"{channel_username}_{message_id}").set(post)