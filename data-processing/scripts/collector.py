import requests
import time
import json
import os
import boto3
from datetime import datetime, timezone

HEADERS = {"User-Agent": "cs-career-intel/1.0 (big data project)"}
SUBREDDITS = ["csMajors", "cscareerquestions"]
LISTINGS = ["new", "top"]
MAX_PAGES = 10
LIMIT_PER_PAGE = 100
S3_BUCKET = "bigdata-cs-careers"


def fetch_posts(subreddit, listing="new", limit_per_page=LIMIT_PER_PAGE, max_pages=MAX_PAGES):
    posts = []
    after = None

    for _ in range(max_pages):
        url = f"https://www.reddit.com/r/{subreddit}/{listing}.json"
        params = {"limit": limit_per_page}
        if after:
            params["after"] = after

        # Retry logic with exponential backoff for 429 rate limiting
        for attempt in range(5):
            res = requests.get(url, headers=HEADERS, params=params)
            if res.status_code == 200:
                break
            if res.status_code == 429:
                wait = 2 ** attempt
                print(f"  Rate limited (429). Waiting {wait}s before retry {attempt + 1}/5...")
                time.sleep(wait)
            else:
                print(f"  HTTP {res.status_code} on r/{subreddit}/{listing} — stopping pagination")
                return posts
        else:
            print(f"  Max retries exceeded for r/{subreddit}/{listing} — stopping pagination")
            return posts

        data = res.json()["data"]
        batch = data["children"]

        if not batch:
            break

        for item in batch:
            p = item["data"]
            posts.append({
                "id": p["id"],
                "title": p["title"],
                "selftext": p.get("selftext", ""),
                "score": p["score"],
                "created_utc": p["created_utc"],
                "num_comments": p["num_comments"],
                "url": p["url"],
                "subreddit": subreddit,
            })

        after = data.get("after")
        if not after:
            break

        time.sleep(1)

    return posts


def collect_all_posts():
    all_posts = []
    seen_ids = set()

    for sub in SUBREDDITS:
        for listing in LISTINGS:
            print(f"Fetching r/{sub}/{listing}...")
            batch = fetch_posts(sub, listing=listing)
            # Deduplicate by post ID
            new_posts = [p for p in batch if p["id"] not in seen_ids]
            seen_ids.update(p["id"] for p in new_posts)
            all_posts.extend(new_posts)
            print(f"  +{len(new_posts)} posts (total: {len(all_posts)})")
            time.sleep(2)

    return all_posts


def upload_to_s3(posts, date_str):
    """Upload posts and metadata to s3://bigdata-cs-careers/raw/YYYY-MM-DD/"""
    s3 = boto3.client("s3")
    s3_prefix = f"raw/{date_str}"

    posts_key = f"{s3_prefix}/posts.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=posts_key,
        Body=json.dumps(posts, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )
    print(f"Uploaded s3://{S3_BUCKET}/{posts_key}")

    metadata = {
        "collection_date": date_str,
        "subreddits": SUBREDDITS,
        "listings": LISTINGS,
        "total_posts": len(posts),
        "collected_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    metadata_key = f"{s3_prefix}/metadata.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=metadata_key,
        Body=json.dumps(metadata).encode("utf-8"),
        ContentType="application/json",
    )
    print(f"Uploaded s3://{S3_BUCKET}/{metadata_key}")

    return metadata


def lambda_handler(event, context):
    """AWS Lambda entrypoint — triggered daily by CloudWatch Events."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    print(f"Starting collection for {today}")

    posts = collect_all_posts()
    print(f"Collected {len(posts)} unique posts")

    metadata = upload_to_s3(posts, today)
    return {"statusCode": 200, "body": json.dumps(metadata)}


if __name__ == "__main__":
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    posts = collect_all_posts()
    print(f"\nTotal unique posts collected: {len(posts)}")

    # Save locally for the processor
    os.makedirs("data", exist_ok=True)

    dated_path = f"data/raw_posts_{today}.json"
    with open(dated_path, "w", encoding="utf-8") as f:
        json.dump(posts, f, ensure_ascii=False)
    print(f"Saved to {dated_path}")

    latest_path = "data/raw_posts.json"
    with open(latest_path, "w", encoding="utf-8") as f:
        json.dump(posts, f, ensure_ascii=False)
    print(f"Saved to {latest_path} (for processor)")

    # Upload to S3
    upload_to_s3(posts, today)
