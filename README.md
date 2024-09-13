# AbuSegal Express

AbuSegal Express is a Google Cloud Function that fetches recent posts from specified Telegram channels and displays them in a clean, Telegram-like web interface. This project is ideal for aggregating content from multiple Telegram channels into a single, easy-to-read feed.

## Features

- Fetches posts from multiple Telegram channels
- Displays posts in a Telegram-like interface
- Automatically updates with new posts
- Filters out sponsored content
- Supports right-to-left (RTL) text for Hebrew content

## Prerequisites

- Google Cloud Platform account
- Python 3.7 or higher
- `gcloud` CLI tool installed and configured

## Setup and Deployment

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/AbuSegal-Express.git
   cd AbuSegal-Express
   ```

2. Create a `requirements.txt` file with the following content:
   ```
   functions-framework==3.*
   google-cloud-firestore==2.*
   requests==2.*
   beautifulsoup4==4.*
   pytz==2022.*
   ```

3. Set up a Google Cloud project and enable the necessary APIs (Cloud Functions, Firestore).

4. Create an API key for your project:
   - Go to the Google Cloud Console
   - Navigate to "APIs & Services" > "Credentials"
   - Click "Create Credentials" > "API Key"
   - Copy the generated API key

5. Set up environment variables:
   ```
   gcloud functions deploy AbuSegal_Express --set-env-vars API_KEY=your_api_key_here
   ```

6. Test locally the function
   ```
   gcloud auth application-default login
   export API_KEY=your_secret_api_key
   functions-framework --target func_name --debug
   curl "[http://localhost:8080?api_key=YOUR_SECRET_API_KEY]"
              (or just browse: http://localhost:8080/?api_key=YOUR_SECRET_API_KEY)
   ```
   
8. Deploy the function:
   ```
   gcloud functions deploy AbuSegal_Express --runtime python310 --trigger-http --allow-unauthenticated
   ```

9. After deployment, you'll receive a URL for your function. You can access it by appending your API key as a query parameter:
   ```
   https://your-function-url?api_key=your_api_key_here
   ```

## Configuration

To change the Telegram channels being monitored, edit the `CHANNEL_USERNAMES` list in the `utils.py` file.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
