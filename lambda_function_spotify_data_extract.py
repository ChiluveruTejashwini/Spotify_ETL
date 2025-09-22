import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
    # --- Spotify Auth ---
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')

    client_credentials_manager = SpotifyClientCredentials(
        client_id=client_id,
        client_secret=client_secret
    )
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    # --- Extract Spotify Playlist Data ---
    playlist_link = "https://open.spotify.com/playlist/1ylsIvgoiYRTF1vMqQffcB?si=nKodKkIrS3-kps3hSAUmnA"
    playlist_URI = playlist_link.split("/")[-1].split("?")[0]
    spotify_data = sp.playlist_tracks(playlist_URI)

    # --- Transform: Keep useful fields ---
    tracks = []
    for item in spotify_data['items']:
        track = item['track']
        tracks.append({
            "track_name": track['name'],
            "artist": track['artists'][0]['name'],
            "album": track['album']['name'],
            "release_date": track['album']['release_date'],
            "duration_ms": track['duration_ms'],
            "added_at": item['added_at']
        })

    # --- Save to S3 ---
    client = boto3.client('s3')
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"spotify_raw_{timestamp}.json"

    client.put_object(
        Bucket="spotify-project-tejaswini",
        Key=f"raw_data/to_processed/{filename}",
        Body=json.dumps(tracks, indent=2)
    )

    return {
        'statusCode': 200,
        'body': json.dumps(f"Uploaded {len(tracks)} tracks to S3 as {filename}")
    }
