from datetime import datetime, timedelta
from google.cloud import firestore
import re
from utils import CHANNEL_USERNAMES, israel_tz

def get_combined_feed(db, israel_tz):
    yesterday = datetime.now(israel_tz) - timedelta(days=1)
    posts_ref = db.collection('posts')
    query = posts_ref.where('date', '>', yesterday).order_by('date', direction=firestore.Query.DESCENDING)
    return [doc.to_dict() for doc in query.stream()]

def format_as_html(posts):
    html = '''
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            /* Telegram-like background and styles */
            html, body {
                height: 100%;  /* Ensure both html and body take full height */
                margin: 0;  /* Remove default margins */
                padding: 0;
                font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif; 
                background-color: #eaeaea; 
                background-image: url('https://i.imgur.com/7JtPOg5.png');  /* Direct URL to image */
                background-size: cover; 
                background-repeat: no-repeat;  /* Ensure no repeating of the background */
                background-attachment: fixed;  /* Keep background in place while scrolling */
                background-position: center center;  /* Center the image */
            }

            .container {
                max-width: 700px;  /* Constrain the width of the container */
                margin: 0 auto;  /* Center the container */
                padding: 20px;  /* Padding around the content */
            }

            .post { 
                background-color: white; 
                border-radius: 8px; 
                padding: 15px; 
                margin-bottom: 15px; 
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);  /* Similar Telegram shadow effect */
                max-width: 100%;  /* Ensure the posts fit within the container */
            }

            .channel-0 { 
                font-weight: bold; 
                color: #8B0000;  /* Dark Red for the first channel */
                margin-bottom: 5px; 
            }

            .channel-1 { 
                font-weight: bold; 
                color: #00008B;  /* Dark Blue for the second channel */
                margin-bottom: 5px; 
            }

            .channel-2 { 
                font-weight: bold; 
                color: #006400;  /* Dark Green for the third channel */
                margin-bottom: 5px; 
            }

            .channel-3 { 
                font-weight: bold; 
                color: #FF8C00;  /* Orange for the fourth channel */
                margin-bottom: 5px; 
            }

            .channel-4 { 
                font-weight: bold; 
                color: black;  /* Black for the fifth channel */
                margin-bottom: 5px; 
            }

            .date { 
                color: #999; 
                font-size: 0.85em; 
                margin-bottom: 10px; 
            }

            .rtl { 
                direction: rtl; 
                text-align: right;  /* RTL formatting for Hebrew */
            }

            a { 
                color: #4a76a8; 
                text-decoration: none; 
            }

            a:hover { 
                text-decoration: underline; 
            }

            /* Style for images */
            .post img {
                max-width: 100%;
                height: auto;
                margin-top: 10px;
                border-radius: 8px;
                box-shadow: 0 1px 2px rgba(0,0,0,0.1);
            }
        </style>
        <script>
            window.onload = function() {
                window.scrollTo(0, document.body.scrollHeight);
            };
        </script>
    </head>
    <body>
        <div class="container">
    '''
    sorted_posts = sorted(posts, key=lambda x: x['date'])
    cleaned_posts = []
    skip_next = False
    for post in sorted_posts:
        if skip_next:
            skip_next = False
            continue
        if "תוכן שיווקי" in post['text'] or "תוכן ממומן" in post['text']:
            skip_next = True
            continue
        cleaned_posts.append(post)

    for post in cleaned_posts:
        israel_time = post['date'].astimezone(israel_tz)
        date_str = israel_time.strftime('%d.%m.%y %H:%M')
        channel_username = post['channel_id'].replace('@', '')
        try:
            channel_index = CHANNEL_USERNAMES.index(channel_username) % 5  # Now modulo 5
        except ValueError:
            # If the channel is not in the list, assign a default color
            channel_index = 0  # Or any other default index
        rtl_class = 'rtl' if any('\u0590' <= char <= '\u05FF' for char in post['text']) else ''
        clean_text = post['text'].replace("כדי להגיב לכתבה לחצו כאן", "")
        clean_text = re.sub(r'(#{2,})', r'<br>\1<br>', clean_text)
        clean_text = re.sub(r'(https?://[^\s]+)', r'<a href="\1" target="_blank">\1</a>', clean_text)

        html += f'''
        <div class="post {rtl_class}">
            <div class="channel-{channel_index}">{post["channel_id"]}</div>
            <div class="date">{date_str}</div>
            <p>{clean_text}</p>
        '''

        if 'images' in post and post['images']:
            for img_url in post['images']:
                html += f'<img src="{img_url}" alt="Telegram Image"/>'

        if 'video_url' in post:
            video_url = post['video_url']
            html += f'<br><a href="{video_url}" target="_blank">Watch Video</a>'

        if 'files' in post:
            for file_url in post['files']:
                html += f'<br><a href="{file_url}" target="_blank">Download File</a>'

        html += '</div>'

    html += '''
        </div>
    </body>
    </html>
    '''
    
    return html
