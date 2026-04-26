import requests
import time
import json
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone

HEADERS = {"User-Agent": "cs-career-intel/1.0 (big data project)"}
SUBREDDITS = ["csMajors", "cscareerquestions"]
LISTINGS = ["new", "top"]
MAX_PAGES = 10
LIMIT_PER_PAGE = 100
S3_BUCKET = "bigdata-cs-careers"
SEEN_IDS_KEY = "metadata/seen_ids_.json"


def load_seen_ids():
    """Download the set of all previously collected post IDs from S3."""
    try:
        response = boto3.client("s3").get_object(Bucket=S3_BUCKET, Key=SEEN_IDS_KEY)
        return set(json.loads(response["Body"].read().decode("utf-8")))
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return set()
        raise


def save_seen_ids(seen_ids):
    """Upload the updated set of all collected post IDs to S3."""
    boto3.client("s3").put_object(
        Bucket=S3_BUCKET,
        Key=SEEN_IDS_KEY,
        Body=json.dumps(list(seen_ids)).encode("utf-8"),
        ContentType="application/json",
    )
    print(f"Updated seen_ids: {len(seen_ids)} total IDs tracked")


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


def collect_all_posts(global_seen_ids=None):
    all_posts = []
    seen_ids = set(global_seen_ids) if global_seen_ids else set()

    for sub in SUBREDDITS:
        for listing in LISTINGS:
            print(f"Fetching r/{sub}/{listing}...")
            batch = fetch_posts(sub, listing=listing)
            new_posts = [p for p in batch if p["id"] not in seen_ids]
            seen_ids.update(p["id"] for p in new_posts)
            all_posts.extend(new_posts)
            print(f"  +{len(new_posts)} new posts (total: {len(all_posts)})")
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


if __name__ == "__main__":
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    print(f"Starting collection for {today}")

    print("\nLoading previously seen post IDs from S3...")
    seen_ids = load_seen_ids()
    print(f"  {len(seen_ids)} IDs already tracked across all previous runs")

    posts = collect_all_posts(seen_ids)
    print(f"\nNew unique posts collected: {len(posts)}")

    if posts:
        upload_to_s3(posts, today)
        seen_ids.update(p["id"] for p in posts)
        save_seen_ids(seen_ids)
    else:
        print("No new posts to upload — all fetched posts were already collected.")
    print("Done.")
