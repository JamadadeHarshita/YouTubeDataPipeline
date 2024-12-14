from google.auth import default
from googleapiclient.discovery import build
from google.cloud import storage
from datetime import datetime
import json

GCP_PROJECT_ID = "rising-method-443800-r1"
GCS_BUCKET_NAME = "sidemen-bucket"

SCOPES = ['https://www.googleapis.com/auth/youtube.readonly']
CHANNEL_IDS = {
    "Sidemen": "UCDogdKl7t7NHzQ95aEwkdMw",
    "Sidemen Reacts": "UCjRkTl_HP4zOh3UFaThgRZw",
    "More Sidemen": "UCh5mLn90vUaB1PbRRx_AiaA",
    "Sidemen Shorts": "UCbAZH3nTxzyNmehmTUhuUsA"
}

def get_authenticated_service():
    credentials, project_id = default(scopes=SCOPES)
    return build('youtube', 'v3', credentials=credentials)

def upload_to_gcs(bucket_name, data, gcs_file):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_file)
    blob.upload_from_string(json.dumps(data, indent=4), content_type="application/json")
    print(f"Uploaded data to gs://{bucket_name}/{gcs_file}")

def fetch_channel_stats(youtube, channel_id):

    try:
        response = youtube.channels().list(
            part="statistics,snippet,brandingSettings,topicDetails",
            id=channel_id
        ).execute()
        channel_data = response["items"][0]

        snippet = channel_data.get("snippet", {})
        statistics = channel_data.get("statistics", {})
        topic_details = channel_data.get("topicDetails", {})

        return {
            "channel_id": channel_id,
            "title": snippet.get("title", "Unknown"),
            "subscribers": int(statistics.get("subscriberCount", 0)),
            "total_views": int(statistics.get("viewCount", 0)),
            "video_count": int(statistics.get("videoCount", 0)),
            "description": snippet.get("description", ""),
            "country": snippet.get("country", "Unknown"),
            "thumbnail": snippet.get("thumbnails", {}).get("default", {}).get("url", "No Thumbnail"),
            "topics": topic_details.get("topicCategories", [])
        }
    except Exception as e:
        print(f"Error fetching channel stats for {channel_id}: {e}")
        return {}

def fetch_video_ids(youtube, channel_id):

    try:
        response = youtube.channels().list(
            part="contentDetails",
            id=channel_id
        ).execute()
        uploads_playlist_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
        
        video_ids = []
        next_page_token = None
        while True:
            playlist_response = youtube.playlistItems().list(
                part="contentDetails",
                playlistId=uploads_playlist_id,
                maxResults=50,
                pageToken=next_page_token
            ).execute()
            video_ids.extend([item["contentDetails"]["videoId"] for item in playlist_response["items"]])
            next_page_token = playlist_response.get("nextPageToken")
            if not next_page_token:
                break
        return video_ids
    except Exception as e:
        print(f"Error fetching video IDs for channel {channel_id}: {e}")
        return []

def fetch_video_analytics(youtube, video_id):
    try:
        response = youtube.videos().list(
            part="snippet,statistics,contentDetails",
            id=video_id
        ).execute()
        video_data = response["items"][0]
        stats = video_data["statistics"]
        snippet = video_data["snippet"]
        content_details = video_data.get("contentDetails", {})
        return {
            "title": snippet.get("title", ""),
            "views": int(stats.get("viewCount", 0)),
            "likes": int(stats.get("likeCount", 0)),
            "comments": int(stats.get("commentCount", 0)),
            "category_id": snippet.get("categoryId", "Unknown"),
            "tags": snippet.get("tags", []),
            "description": snippet.get("description", ""),
            "published_date": snippet.get("publishedAt", ""),
            "duration": content_details.get("duration", "")
            
            
        }
    except Exception as e:
        print(f"Error fetching analytics for video {video_id}: {e}")
        return {}
    

def fetch_youtube_data(request):

    youtube = get_authenticated_service()

    #Fetching and uploading channel data 
    channel_stats = []
    for channel_name, channel_id in CHANNEL_IDS.items():
        print(f"Fetching statistics for {channel_name}...")
        stats = fetch_channel_stats(youtube, channel_id)
        channel_stats.append(stats)
    upload_to_gcs(GCS_BUCKET_NAME, channel_stats, "youtube/channel_stats.json")

    #Fetching  and uploading video level data 
    video_analytics = []
    for channel_name, channel_id in CHANNEL_IDS.items():
        print(f"Fetching videos for {channel_name}...")
        video_ids = fetch_video_ids(youtube, channel_id)
        for video_id in video_ids:
            analytics = fetch_video_analytics(youtube, video_id)
            video_analytics.append({"channel_name": channel_name, "video_id": video_id, **analytics})
    upload_to_gcs(GCS_BUCKET_NAME, video_analytics, "youtube/video_analytics.json")

    print("YouTube data is successfully fetched and uploaded to GCS.")
    return "YouTube data is successfully fetched and uploaded to GCS."
