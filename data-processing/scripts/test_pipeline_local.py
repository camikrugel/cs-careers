import json
import pandas as pd
from pyspark.sql import SparkSession

def test_pipeline_locally():
    """Test the pipeline with sample data locally before running on S3"""
    
    # Create sample Reddit data
    sample_data = [
        {
            "id": "test1",
            "title": "Got an internship offer at Amazon for $45/hr!",
            "selftext": "Super excited! My leetcode grind paid off. Any advice for someone starting?",
            "author": "user1",
            "subreddit": "csMajors",
            "score": 150,
            "num_comments": 45,
            "created_utc": 1704067200
        },
        {
            "id": "test2",
            "title": "Interview tips for new grad positions?",
            "selftext": "I have interviews coming up with Google and Microsoft. What should I focus on? Data structures and algorithms?",
            "author": "user2",
            "subreddit": "cscareerquestions",
            "score": 89,
            "num_comments": 32,
            "created_utc": 1704153600
        },
        {
            "id": "test3",
            "title": "Feeling depressed about job search",
            "selftext": "Applied to 200+ companies, only got 3 interviews. The market is brutal.",
            "author": "user3",
            "subreddit": "csMajors",
            "score": 234,
            "num_comments": 78,
            "created_utc": 1704240000
        }
    ]
    
    # Save to local JSON
    with open('test_data.json', 'w') as f:
        for record in sample_data:
            f.write(json.dumps(record) + '\n')
    
    # Create local Spark session
    spark = SparkSession.builder \
        .appName("Local Pipeline Test") \
        .master("local[*]") \
        .getOrCreate()
    
    # Load and process
    df = spark.read.json('test_data.json')
    
    print("Original data:")
    df.show(truncate=False)
    
    print("\nRunning pipeline steps...")
    # Import and run your pipeline functions here
    # from process_reddit_data import clean_text_pipeline, classify_topics, etc.
    
    spark.stop()

if __name__ == "__main__":
    test_pipeline_locally()