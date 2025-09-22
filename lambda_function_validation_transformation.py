import json
import boto3
from datetime import datetime
from collections import defaultdict

s3 = boto3.client("s3")

def lambda_handler(event, context):
    bucket = "spotify-project-tejaswini"
    raw_prefix = "raw_data/to_processed/"
    processed_prefix = "processed/"
    tracklist = "tracklist/"

    # --- Step 1: Get latest raw file ---
    response = s3.list_objects_v2(Bucket=bucket, Prefix=raw_prefix)
    if "Contents" not in response:
        return {"statusCode": 404, "body": "No raw files found."}

    # Pick latest file by LastModified
    latest_file = max(response["Contents"], key=lambda x: x["LastModified"])["Key"]

    print(f"Processing file: {latest_file}")

    raw_obj = s3.get_object(Bucket=bucket, Key=latest_file)
    raw_data = json.loads(raw_obj["Body"].read().decode("utf-8"))

    # --- Step 2: Transform data ---
    processed_data = []
    di=defaultdict(list)
    for track in raw_data:
        processed_data.append({
            "track_name": track.get("track_name"),
            "artist": track.get("artist"),
            "album": track.get("album"),
            "release_date": track.get("release_date"),
            "duration_sec": round(track.get("duration_ms", 0) / 1000, 2),
            "ingested_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        di["track_name"].append(track.get("track_name"))
        di["artist"].append(track.get("artist"))
        di["album"].append(track.get("album"))

    # --- Step 3: Validations ---
    errors = []

    # 1) Check for exactly 7 records
    if len(processed_data) != 7:
        errors.append(f"Expected 7 records, found {len(processed_data)}")

    # 2) Check title contains "chuttamalle" (case-insensitive, allows suffixes like "(From Devara Part 1)")
    has_chuttamalle = any(
        t.get("track_name") and "chuttamalle" in t["track_name"].lower()
        for t in processed_data
    )
    if not has_chuttamalle:
        errors.append("Missing required track with title containing 'chuttamalle'")

    # If validations fail → stop processing
    if errors:
        print("Validation failed:", errors)
        return {
            "statusCode": 400,
            "body": f"Validation failed: {errors}"
        }

    # --- Step 4: Save to processed/ ---
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    processed_filename = f"{processed_prefix}spotify_processed_{timestamp}.json"
    trackname=f"{tracklist}spotify_Track{timestamp}.json"

    s3.put_object(
        Bucket=bucket,
        Key=processed_filename,
        Body=json.dumps(processed_data, indent=2)
    )
    s3.put_object(
        Bucket=bucket,
        Key=trackname,
        Body=json.dumps(list(di.items()), indent=2)
    )

    return {
        "statusCode": 200,
        "body": f"Processed {len(processed_data)} tracks and saved to {processed_filename}"
    }
