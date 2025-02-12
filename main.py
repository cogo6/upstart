import boto3
import os
import json
import time
import random
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from dotenv import load_dotenv
import urllib.parse
import datetime
from datetime import datetime, timezone, timedelta
#hi
load_dotenv()

# Constants
SCOPES = ["https://www.googleapis.com/auth/youtube.upload", "https://www.googleapis.com/auth/youtube.readonly"]
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
TEMP_VIDEO_PATH = "downloaded_video.mp4"
TEMP_META_JSON = "temp_metadata.json"

def initialize_globals(config):
    """
    Initializes global variables from the provided configuration dictionary.
    """
    global CHANNEL, BUCKET_NAME, FOLDERS
    global CLIENT_SECRET_FILE, CREDENTIALS_FILE
    global PRIVACY_STATUS, UPLOADED_FOLDER, CATEGORY_ID

    CHANNEL = config["channel"]
    BUCKET_NAME = config["bucket_name"]
    FOLDERS = config["folders"]
    CLIENT_SECRET_FILE = config["client_secret_file"]
    CREDENTIALS_FILE = config["credentials_file"]
    PRIVACY_STATUS = config["privacy_status"]
    CATEGORY_ID = config["category_id"]
    print(CLIENT_SECRET_FILE)

    # Default to 'uploaded/' if not specified in the config
    UPLOADED_FOLDER = config.get("uploaded_folder", "uploaded/")

def get_random_cap_video(s3_client):
    """
    Retrieves a random video whose key ends with '_CAP.mp4'
    from the specified folders in the S3 bucket.
    """
    all_cap_videos = []
    for folder in FOLDERS:
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=folder)
        if "Contents" in response:
            # Filter for files that end with '_CAP.mp4'
            for obj in response["Contents"]:
                key = obj["Key"]
                if key.endswith("_CAP.mp4"):
                    all_cap_videos.append(key)

    if not all_cap_videos:
        raise Exception("No videos found in the specified folders matching '*_CAP.mp4'.")

    return random.choice(all_cap_videos)

def download_from_s3(s3_client, s3_key, download_path):
    """
    Downloads a file from S3 to the specified local path.
    """
    print(f"Downloading {s3_key} from S3...")
    s3_client.download_file(BUCKET_NAME, s3_key, download_path)
    print(f"Downloaded to {download_path}")

def get_video_metadata(s3_client, video_key):
    """
    Retrieves metadata for a video from the 'videos_metadata.json'
    in the same folder as the video_key.
    Returns the dictionary of metadata for that specific video ID,
    or None if not found.
    """
    folder = os.path.dirname(video_key)  # Something like "interestingasfuck"
    # Ensure forward slash for S3
    if folder:
        metadata_file_key = f"{folder}/videos_metadata.json"
    else:
        metadata_file_key = "videos_metadata.json"

    print(f"Downloading metadata file from S3: {metadata_file_key}...")
    try:
        s3_client.download_file(BUCKET_NAME, metadata_file_key, TEMP_META_JSON)
    except Exception as e:
        print(f"Could not download {metadata_file_key}: {e}")
        return None

    with open(TEMP_META_JSON, "r", encoding="utf-8") as f:
        all_metadata = json.load(f)

    # For example "abc123_CAP.mp4" -> "abc123_CAP"
    base_filename = os.path.basename(video_key).replace("_CAP.mp4", "")
    video_metadata = all_metadata.get(base_filename)

    if os.path.exists(TEMP_META_JSON):
        os.remove(TEMP_META_JSON)

    return video_metadata

def upload_to_youtube(video_path, metadata, config):
    """
    Uploads a video to YouTube using refreshed OAuth credentials.
    If credentials are expired, refreshes them automatically.
    Only forces a full OAuth re-authentication if the refresh token is missing or invalid.
    """
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
    from datetime import datetime, timezone, timedelta
    import time
    import os

    print("🔑 Authenticating with YouTube API...")

    # Load config values
    CREDENTIALS_FILE = config["credentials_file"]
    CLIENT_SECRET_FILE = config["client_secret_file"]
    PRIVACY_STATUS = config["privacy_status"]
    CATEGORY_ID = str(config.get("category_id", 22))
    SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]

    creds = None

    # ✅ Load existing credentials (avoid unnecessary re-authentication)
    if os.path.exists(CREDENTIALS_FILE):
        creds = Credentials.from_authorized_user_file(CREDENTIALS_FILE, SCOPES)

    # ✅ Refresh token only if credentials are expired
    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())  # Refreshes token silently
            print("✅ Token successfully refreshed.")
        except Exception as e:
            print(f"⚠️ Token refresh failed: {e}")
            # Only force re-authentication if there's no valid token at all
            if not creds.valid:
                print("🔄 No valid credentials. Re-authenticating...")
                creds = None

                # ✅ Only run OAuth flow if we have no valid credentials (last resort)
    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
        creds = flow.run_local_server(port=0)  # User logs in

    # ✅ Save refreshed credentials
    with open(CREDENTIALS_FILE, "w", encoding="utf-8") as token_file:
        token_file.write(creds.to_json())

    # ✅ Build YouTube API client
    youtube = build("youtube", "v3", credentials=creds)

    # Prepare video metadata
    title = metadata.get("ytitle", metadata.get("title", "Untitled"))
    tags = metadata.get("tags", [])
    description = metadata.get("title", "desc.")

    request_body = {
        "snippet": {
            "title": title[:100],  # Enforce 100-char limit
            "description": description,
            "tags": tags,
            "categoryId": CATEGORY_ID
        },
        "status": {
            "privacyStatus": PRIVACY_STATUS,  # "public", "unlisted", or "private"
            "selfDeclaredMadeForKids": False
        }
    }

    # ✅ Handle scheduled uploads if "private"
    if PRIVACY_STATUS == "private":
        upload_time_str = config.get("upload_time", "10:00")
        upload_delay_days = int(config.get("upload_delay_days", "0"))

        now_utc = datetime.now(timezone.utc)
        target_date = now_utc + timedelta(days=upload_delay_days)

        hour, minute = map(int, upload_time_str.split(":"))
        scheduled_datetime = target_date.replace(hour=hour, minute=minute, second=0, microsecond=0)

        publish_at_iso = scheduled_datetime.isoformat()

        request_body["status"]["privacyStatus"] = "private"
        request_body["status"]["publishAt"] = publish_at_iso

        print(f"📅 Video scheduled for: {publish_at_iso}")

    print(f"🎬 Uploading video: {request_body['snippet']['title']}")

    # Upload video file
    media = MediaFileUpload(video_path, chunksize=-1, resumable=True)
    request = youtube.videos().insert(
        part="snippet,status",
        body=request_body,
        media_body=media
    )

    # Retry logic for transient failures
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        try:
            response = request.execute()
            video_id = response["id"]
            print(f"✅ Video uploaded ssuccessfully: https://youtu.be/{video_id}")
            return video_id
        except Exception as e:
            print(f"⚠️ [Attempt {attempt}/{max_attempts}] Upload error: {e}")
            if attempt == max_attempts:
                print("❌ Maximum retries reached. Upload failed.")
                raise
            wait_seconds = 5 * attempt
            print(f"🔄 Retrying in {wait_seconds} seconds...\n")
            time.sleep(wait_seconds)

def save_upload_info(s3_client, video_key, youtube_id, video_metadata, config):
    """
    Saves or updates a JSON file in the 'uploaded' folder with info about the
    newly uploaded video. If a JSON file for this video already exists, download
    it, update it, and re-upload it.
    """
    # We construct the JSON filename to be the same as the video, but .json
    base_name = os.path.splitext(os.path.basename(video_key))[0]  # e.g. 'abc123_CAP'
    json_key = os.path.join(UPLOADED_FOLDER, base_name + ".json")

    print(f"Preparing to update or create JSON metadata at: {json_key}")

    # Attempt to download any existing JSON for this video
    local_json_path = "temp_uploaded_info.json"
    existing_data = {}

    try:
        s3_client.download_file(BUCKET_NAME, json_key, local_json_path)
        print(f"Existing JSON found at {json_key}. Will update it.")
        with open(local_json_path, "r", encoding="utf-8") as f:
            existing_data = json.load(f)
    except Exception:
        print("No existing JSON file found. A new one will be created.")

    # Merge or create new data
    # For example, we store YouTube ID, channel, video_metadata, date, etc.
    new_info = {
        "youtube_id": youtube_id,
        "channel": config["channel"],
        "privacy_status": config.get("privacy_status", "unlisted"),
        "upload_time": time.strftime("%Y-%m-%d %H:%M:%S"),
        "video_metadata": video_metadata
    }

    # Update or replace the existing_data with new_info
    # If you need to preserve certain old fields, do so here;
    # otherwise we simply overwrite.
    existing_data.update(new_info)

    # Write back to local JSON
    with open(local_json_path, "w", encoding="utf-8") as f:
        json.dump(existing_data, f, indent=4)

    # Upload it to S3
    print("Uploading updated JSON back to S3...")
    s3_client.upload_file(local_json_path, BUCKET_NAME, json_key)
    print("JSON metadata file updated successfully.")

    if os.path.exists(local_json_path):
        os.remove(local_json_path)

def move_uploaded_video(s3_client, original_key):
    """
    Moves the uploaded video to the 'uploaded' folder in S3.
    """
    filename = os.path.basename(original_key)
    new_key = os.path.join(UPLOADED_FOLDER, filename)

    print(f"Copying {original_key} to {new_key} in S3...")
    s3_client.copy_object(
        Bucket=BUCKET_NAME,
        CopySource={"Bucket": BUCKET_NAME, "Key": original_key},
        Key=new_key
    )
    s3_client.delete_object(Bucket=BUCKET_NAME, Key=original_key)
    print(f"Video moved to {new_key}.")

def get_video_id_from_url(video_url):
    """
    Extracts a video ID from either a standard YouTube link with ?v=,
    or from a Shorts link with /shorts/<ID>.
    """
    parsed = urllib.parse.urlparse(video_url)
    query_params = urllib.parse.parse_qs(parsed.query)

    # 1) Check if there's a "v" parameter
    if "v" in query_params and query_params["v"]:
        return query_params["v"][0]

    # 2) Otherwise, check if it's a Shorts URL of the form /shorts/<ID>
    path_parts = parsed.path.strip("/").split("/")
    if len(path_parts) == 2 and path_parts[0] == "shorts":
        return path_parts[1]

    # If none matched, we can't parse the ID
    raise ValueError(f"Could not extract a video ID from URL: {video_url}")

def get_video_snippet_oauth(video_url):
    """
    Given a YouTube video URL (including Shorts), authenticate via OAuth
    (using the global CREDENTIALS_FILE and CLIENT_SECRET_FILE) and retrieve
    the snippet info (title, description, tags, categoryId, etc.) for that video.
    """
    # 1. Extract the video ID
    video_id = get_video_id_from_url(video_url)

    # 2. Load or create OAuth credentials from global files
    if os.path.exists(CREDENTIALS_FILE):
        credentials = Credentials.from_authorized_user_file(CREDENTIALS_FILE, SCOPES)
    else:
        flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
        credentials = flow.run_local_server(port=0)
        with open(CREDENTIALS_FILE, "w", encoding="utf-8") as cred_file:
            cred_file.write(credentials.to_json())

    # 3. Build the YouTube API client
    youtube = build("youtube", "v3", credentials=credentials)

    # 4. Query the snippet
    response = youtube.videos().list(
        part="snippet",
        id=video_id
    ).execute()

    items = response.get("items", [])
    if not items:
        raise ValueError(f"No video found (or it's private). ID: {video_id}")

    # 5. Return snippet portion
    snippet = items[0]["snippet"]
    return snippet


def main(config_file):
    """
    Main workflow for processing and uploading a random *_CAP.mp4 video.
    """
    # 1. Load the user-provided config
    with open(config_file, "r", encoding="utf-8") as file:
        config = json.load(file)

    # 2. Initialize global variables
    initialize_globals(config)

    # 3. Create an S3 client
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )

    try:
        # a) Select a random *_CAP.mp4
        cap_video_key = get_random_cap_video(s3_client)
        video_filename = os.path.basename(cap_video_key)
        local_video_path = TEMP_VIDEO_PATH

        # b) Download the video
        download_from_s3(s3_client, cap_video_key, local_video_path)

        # c) Get the per-video metadata from videos_metadata.json
        video_metadata = get_video_metadata(s3_client, cap_video_key)
        if not video_metadata:
            raise Exception("No metadata found for this video in videos_metadata.json.")

        # d) Upload the video to YouTube
        youtube_id = upload_to_youtube(local_video_path, video_metadata, config)

        # e) Save all info to a JSON in the uploaded folder (create or update)
        save_upload_info(s3_client, cap_video_key, youtube_id, video_metadata, config)

        # f) Move the video file to 'uploaded' folder
        move_uploaded_video(s3_client, cap_video_key)

        # g) Cleanup local video file
        if os.path.exists(local_video_path):
            os.remove(local_video_path)
            print("Local video file cleaned up.")

    except Exception as e:
        print(f"Error during processing: {e}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python upload_video.py <config.json>")
    else:
        main(sys.argv[1])
