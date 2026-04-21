from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, regexp_replace, split, explode, count, desc,
    from_unixtime, year, month, dayofmonth, hour, length, date_format, 
    when, trim, udf, collect_list, avg, sum as spark_sum, lit, size, row_number,
)
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, IntegerType, DoubleType
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.sql.window import Window
from pyspark.ml import Pipeline
import re
import json
import os
import io
import sys
import argparse
import boto3
from botocore import UNSIGNED
from botocore.config import Config
from datetime import datetime, timezone
from pathlib import Path

if sys.platform.startswith('win'):
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    print(f" Windows detected - using Python from: {sys.executable}")

S3_BUCKET = "bigdata-cs-careers"

parser = argparse.ArgumentParser(description='Process Reddit career data with PySpark')
parser.add_argument('--date', type=str,
                    default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                    help='Date to process in YYYY-MM-DD format (default: today UTC)')
args = parser.parse_args()
DATE_STR = args.date


def load_from_s3(date_str):
    """Read raw posts JSON directly from S3 into memory."""
    s3_key = f"raw/{date_str}/posts.json"
    print(f"\n[S3] Reading s3://{S3_BUCKET}/{s3_key} ...")
    try:
        response = boto3.client("s3", config=Config(signature_version=UNSIGNED)).get_object(Bucket=S3_BUCKET, Key=s3_key)
        posts = json.loads(response["Body"].read().decode("utf-8"))
        print(f"     Loaded {len(posts)} posts")
        return posts
    except Exception as e:
        print(f"     ERROR: {e}")
        print(f"     No data found in S3 for {date_str}. Run collector.py first.")
        sys.exit(1)


def upload_csv_to_s3(df, date_str, subfolder):
    """Convert Spark DataFrame to CSV and upload directly to S3."""
    try:
        count = df.count()
        print(f"\n {subfolder}: {count} rows")
        if count == 0:
            print(f"  WARNING: {subfolder} is empty! Skipping.")
            return False
        buf = io.StringIO()
        df.toPandas().to_csv(buf, index=False)
        s3_key = f"processed/{date_str}/{subfolder}/{subfolder}.csv"
        boto3.client("s3").put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=buf.getvalue().encode("utf-8"),
            ContentType="text/csv",
        )
        print(f"  Uploaded s3://{S3_BUCKET}/{s3_key}")
        return True
    except Exception as e:
        print(f"  ERROR uploading {subfolder}: {e}")
        import traceback
        traceback.print_exc()
        return False


def upload_pandas_to_s3(pandas_df, date_str, subfolder):
    """Upload a pandas DataFrame as CSV directly to S3."""
    try:
        if pandas_df.empty:
            print(f"  WARNING: {subfolder} is empty! Skipping.")
            return False
        buf = io.StringIO()
        pandas_df.to_csv(buf, index=False)
        s3_key = f"processed/{date_str}/{subfolder}/{subfolder}.csv"
        boto3.client("s3").put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=buf.getvalue().encode("utf-8"),
            ContentType="text/csv",
        )
        print(f"  Uploaded s3://{S3_BUCKET}/{s3_key} ({len(pandas_df)} rows)")
        return True
    except Exception as e:
        print(f"  ERROR uploading {subfolder}: {e}")
        import traceback
        traceback.print_exc()
        return False


# CONFIGURATION
RAW_POSTS = load_from_s3(DATE_STR)

print(f"\n Processing date: {DATE_STR}")
print(f"   Output: s3://{S3_BUCKET}/processed/{DATE_STR}/")

# Define all possible industries upfront 
ALL_INDUSTRIES = [
    'FAANG/Big Tech',
    'Unicorn Startup',
    'Established Tech',
    'Enterprise/Legacy',
    'Hardware/Semiconductor',
    'FinTech/Finance',
    'Defense/Government',
    'Early Stage Startup',
    'Consulting/Services',
    'E-commerce/Retail',
    'Gaming/Entertainment',
    'Healthcare/Biotech',
    'General/Unspecified'
]

# Define all possible topics upfront
ALL_TOPICS = [
    'Internship',
    'Job Search',
    'Interview Prep',
    'Resume/Profile',
    'Compensation',
    'Big Tech',
    'Education',
    'Seeking Advice',
    'Challenges',
    'General'
]

# Define all possible advice categories upfront
ADVICE_CATEGORIES = {
    "Resume/CV": [
        "resume", "cv", "curriculum vitae", "tailor resume", "resume review",
        "resume tips", "ats", "applicant tracking", "resume format", "resume template"
    ],
    "Interview Prep": [
        "interview prep", "interview tips", "mock interview", "behavioral questions",
        "technical interview", "coding interview", "system design", "whiteboard",
        "interviewing", "interview advice", "how to prepare", "interview practice"
    ],
    "Networking": [
        "networking", "linkedin", "referral", "cold email", "reach out",
        "coffee chat", "informational interview", "connect with", "network with",
        "professional network", "alumni network", "meetup", "career fair"
    ],
    "Job Search Strategy": [
        "job search", "application strategy", "where to apply", "how many applications",
        "job boards", "apply directly", "career site", "job hunting", "application process",
        "mass apply", "targeted applications"
    ],
    "Negotiation": [
        "salary negotiation", "negotiate offer", "counteroffer", "negotiating salary",
        "offer negotiation", "negotiate comp", "ask for more", "negotiation tips",
        "equity negotiation", "sign-on bonus"
    ],
    "Career Transition": [
        "career change", "switch careers", "transition to", "pivot to",
        "changing careers", "new career", "career switch", "bootcamp",
        "self-taught", "career path", "break into"
    ],
    "Skill Development": [
        "learn", "study", "practice", "improve skills", "get better at",
        "learning path", "resources", "courses", "certification", "upskill",
        "side project", "portfolio", "github", "open source"
    ],
    "Work-Life Balance": [
        "work life balance", "wlb", "burnout", "overwork", "work hours",
        "remote work", "flexible hours", "work from home", "mental health",
        "stress", "workload"
    ],
    "Company Research": [
        "research company", "company culture", "glassdoor", "blind",
        "team culture", "work environment", "company reviews", "red flags",
        "green flags", "how to evaluate", "due diligence"
    ]
}
# Start Spark session
print("\n[INITIALIZATION] Starting Spark Session...")

spark = SparkSession.builder \
    .appName("Reddit CS Career Analysis") \
    .master("local[1]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(f" Spark Session initialized")
print(f"  Spark Version: {spark.version}")
print(f"  Master: {spark.sparkContext.master}")
print(f"  Output: s3://{S3_BUCKET}/processed/{DATE_STR}/")



# 1. Load raw data
print(f"\n[STEP 1] Loading raw Reddit data...")

try:
    df = spark.createDataFrame(RAW_POSTS)
    initial_count = df.count()
    print(f" Loaded {initial_count} posts")
    print("\nData Schema:")
    df.printSchema()
except Exception as e:
    print(f" Error loading data: {e}")
    spark.stop()
    sys.exit(1)


# 2. Data cleaning and enrichment
print("\n[STEP 2] Cleaning and enriching data...")

# Convert Unix timestamp to readable datetime
df_cleaned = df.withColumn(
    "created_datetime",
    from_unixtime(col("created_utc"))
)

# Extract date components for time-based analysis
df_cleaned = df_cleaned \
    .withColumn("year", year(col("created_datetime"))) \
    .withColumn("month", month(col("created_datetime"))) \
    .withColumn("day", dayofmonth(col("created_datetime"))) \
    .withColumn("hour", hour(col("created_datetime")))

# Combine title and selftext 
df_cleaned = df_cleaned.withColumn(
    "full_text",
    when(col("selftext").isNull() | (col("selftext") == ""), col("title"))
    .otherwise(regexp_replace(col("title") + " " + col("selftext"), r"\s+", " "))
)

# Calculate text length
df_cleaned = df_cleaned.withColumn(
    "text_length",
    length(col("full_text"))
)

# Categorize engagement
df_cleaned = df_cleaned.withColumn(
    "engagement_level",
    when(col("num_comments") == 0, "No Engagement")
    .when(col("num_comments") <= 2, "Low Engagement")
    .when(col("num_comments") <= 10, "Medium Engagement")
    .otherwise("High Engagement")
)

# Identify if post contains a URL/image
df_cleaned = df_cleaned.withColumn(
    "has_media",
    when(col("url").contains("i.redd.it") | col("url").contains("imgur"), "Image")
    .when(col("url").contains("reddit.com/r/"), "Text Post")
    .otherwise("External Link")
)

cleaned_count = df_cleaned.count()
print(f" Cleaned and enriched {cleaned_count} posts")


# 3. Text preprocessing
print("\n[STEP 3] Preprocessing text for analysis...")

# Clean text: lowercase, remove special characters, URLs
df_text = df_cleaned.withColumn(
    "cleaned_text",
    lower(regexp_replace(
        regexp_replace(
            regexp_replace(col("full_text"), r"http\S+", ""),  # Remove URLs
            r"[^a-zA-Z\s]", " "  # Remove special chars
        ),
        r"\s+", " "  # Normalize whitespace
    ))
)

# Tokenization
tokenizer = Tokenizer(inputCol="cleaned_text", outputCol="words")
df_tokenized = tokenizer.transform(df_text)

# Remove stop words
remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
df_filtered = remover.transform(df_tokenized)

# Filter out very short posts 
df_filtered = df_filtered.filter(length(col("cleaned_text")) > 10)

filtered_count = df_filtered.count()
print(f" Preprocessed {filtered_count} posts")


# 4. Industry Categorization
print("\n[STEP 4] Categorizing by industry type...")

def categorize_industry(text):
    """
    Categorizes posts by industry type based on company mentions.
    Uses case-insensitive matching via .lower()
    """
    if not text:
        return []
    
    text_lower = text.lower()
    industries = []
    
    # FAANG/Big Tech
    faang_keywords = [
        'faang', 'fang', 'big tech', 'big n', 'bigtech',
        'google', 'alphabet', 'meta', 'facebook', 'fb', 'amazon', 'amzn', 'apple', 
        'netflix', 'microsoft', 'msft', 'msn'
    ]
    if any(keyword in text_lower for keyword in faang_keywords):
        industries.append('FAANG/Big Tech')
    
    # Unicorn Startups
    unicorn_keywords = [
        'unicorn', 'stripe', 'databricks', 'figma', 'notion', 'canva', 
        'openai', 'anthropic', 'scale ai', 'scaleai', 'instacart', 'discord',
        'roblox', 'epic games', 'epicgames', 'coinbase', 'robinhood', 'chime'
    ]
    if any(keyword in text_lower for keyword in unicorn_keywords):
        industries.append('Unicorn Startup')
    
    # Established Tech
    established_tech_keywords = [
        'uber', 'lyft', 'airbnb', 'doordash', 'snapchat', 'snap inc',
        'twitter', 'x corp', 'reddit', 'pinterest', 'spotify',
        'salesforce', 'servicenow', 'workday', 'snowflake', 'datadog',
        'twilio', 'cloudflare', 'mongodb', 'elastic', 'atlassian',
        'zoom', 'slack', 'dropbox', 'box', 'square', 'block inc'
    ]
    if any(keyword in text_lower for keyword in established_tech_keywords):
        industries.append('Established Tech')
    
    # Enterprise/Legacy
    enterprise_keywords = [
        'enterprise', 'legacy', 'oracle', 'ibm', 'sap', 'cisco', 'dell', 'hp', 'hpe',
        'vmware', 'broadcom', 'intuit', 'adobe', 'autodesk',
        'red hat', 'redhat', 'splunk', 'symantec', 'mcafee'
    ]
    if any(keyword in text_lower for keyword in enterprise_keywords):
        industries.append('Enterprise/Legacy')
    
    # Hardware/Semiconductor
    hardware_keywords = [
        'hardware', 'semiconductor', 'chip', 'nvidia', 'intel', 'amd', 'qualcomm', 
        'arm', 'tsmc', 'micron', 'texas instruments', 'ti', 'analog devices', 
        'xilinx', 'nxp', 'marvell'
    ]
    if any(keyword in text_lower for keyword in hardware_keywords):
        industries.append('Hardware/Semiconductor')
    
    # FinTech
    fintech_keywords = [
        'fintech', 'finance tech', 'financial', 'trading', 'quant',
        'goldman sachs', 'goldman', 'gs', 'morgan stanley', 'ms',
        'jp morgan', 'jpmorgan', 'jpm', 'chase', 'citi', 'citigroup',
        'bank of america', 'bofa', 'boa', 'capital one', 'american express', 'amex',
        'visa', 'mastercard', 'paypal', 'venmo', 'plaid',
        'bloomberg', 'citadel', 'jane street', 'two sigma', 'twosigma',
        'renaissance', 'rentec', 'bridgewater', 'point72', 'hrt',
        'hudson river trading', 'jump trading', 'optiver', 'imc'
    ]
    if any(keyword in text_lower for keyword in fintech_keywords):
        industries.append('FinTech/Finance')
    
    # Defense/Government
    defense_keywords = [
        'defense', 'govt', 'government', 'federal', 'palantir', 'pltr',
        'lockheed', 'lockheed martin', 'northrop grumman', 'northrop',
        'raytheon', 'rtx', 'boeing', 'general dynamics', 'anduril',
        'clearance', 'secret clearance', 'top secret', 'ts/sci'
    ]
    if any(keyword in text_lower for keyword in defense_keywords):
        industries.append('Defense/Government')
    
    # Early Stage Startup
    startup_keywords = [
        'startup', 'start-up', 'start up', 'early stage', 'seed stage', 
        'series a', 'series b', 'y combinator', 'yc', 'ycombinator',
        'techstars', 'accelerator', 'incubator', 'stealth'
    ]
    if any(keyword in text_lower for keyword in startup_keywords):
        industries.append('Early Stage Startup')
    
    # Consulting/Services
    consulting_keywords = [
        'consulting', 'consultant', 'consultancy', 'accenture', 'deloitte',
        'pwc', 'pricewaterhousecoopers', 'ey', 'ernst & young', 
        'kpmg', 'mckinsey', 'bain', 'bcg', 'boston consulting',
        'capgemini', 'cognizant', 'infosys', 'tcs', 'wipro', 'hcl'
    ]
    if any(keyword in text_lower for keyword in consulting_keywords):
        industries.append('Consulting/Services')
    
    # E-commerce/Retail
    ecommerce_keywords = [
        'ecommerce', 'e-commerce', 'retail', 'shopify', 'walmart', 'target', 
        'best buy', 'bestbuy', 'wayfair', 'etsy', 'ebay', 
        'mercari', 'poshmark', 'stockx', 'goat'
    ]
    if any(keyword in text_lower for keyword in ecommerce_keywords):
        industries.append('E-commerce/Retail')
    
    # Gaming/Entertainment
    gaming_keywords = [
        'gaming', 'game dev', 'gamedev', 'video game', 'unity', 'unreal', 
        'riot games', 'riot', 'blizzard', 'activision', 'activision blizzard',
        'ea', 'electronic arts', 'valve', 'steam', 'nintendo', 
        'sony', 'playstation', 'xbox', 'rockstar', 'supercell'
    ]
    if any(keyword in text_lower for keyword in gaming_keywords):
        industries.append('Gaming/Entertainment')
    
    # Healthcare/Biotech
    healthcare_keywords = [
        'healthcare', 'health tech', 'healthtech', 'biotech', 'pharma', 'medical',
        'teladoc', 'epic systems', 'epic', 'cerner', 'meditech',
        '23andme', 'color genomics', 'grail', 'tempus', 'moderna', 'pfizer'
    ]
    if any(keyword in text_lower for keyword in healthcare_keywords):
        industries.append('Healthcare/Biotech')
    
    return list(set(industries)) if industries else ['General/Unspecified']

# Register UDF
categorize_industry_udf = udf(categorize_industry, ArrayType(StringType()))

# Apply industry categorization
df_filtered = df_filtered.withColumn(
    "industries",
    categorize_industry_udf(col("full_text"))
)

print(" Industry categorization complete")

#Advice categorization
def detect_advice_categories(text):
    """
    Detects advice categories in the text
    Returns a list of detected advice categories
    """
    if not text:
        return []
    
    text_lower = text.lower()
    detected = []
    
    for category, keywords in ADVICE_CATEGORIES.items():
        for keyword in keywords:
            if keyword in text_lower:
                detected.append(category)
                break  # Only add category once even if multiple keywords match
    
    return list(set(detected))  # Remove duplicates

# Register UDF
detect_advice_udf = udf(detect_advice_categories, ArrayType(StringType()))


# 5. Topic Extraction and Keyword Analysis
print("\n[STEP 5] Extracting keywords and topics...")

# Explode words for word frequency analysis
df_words = df_filtered.select(
    "id", "title", "score", "num_comments", "created_datetime",
    explode(col("filtered_words")).alias("word")
)

# Calculate word frequencies
word_freq = df_words.groupBy("word") \
    .agg(count("*").alias("frequency")) \
    .filter(length(col("word")) > 3) \
    .orderBy(desc("frequency"))

print("\n Top 20 Keywords:")
word_freq.show(20, truncate=False)

# Define topic categorization function
def categorize_topic(text):
    if not text:
        return ['General']
    text_lower = text.lower()
    categories = []
    
    if any(word in text_lower for word in ['internship', 'intern', 'summer', 'co-op', 'coop', 'co op']):
        categories.append('Internship')
    
    if any(word in text_lower for word in ['job', 'application', 'apply', 'applying', 'offer', 'search', 'new grad', 'full time', 'fulltime']):
        categories.append('Job Search')
    
    if any(word in text_lower for word in ['interview', 'leetcode', 'oa', 'online assessment', 'assessment', 'coding challenge', 'technical']):
        categories.append('Interview Prep')
    
    if any(word in text_lower for word in ['resume', 'cv', 'portfolio', 'project', 'experience', 'github']):
        categories.append('Resume/Profile')
    
    if any(word in text_lower for word in ['salary', 'compensation', 'pay', 'tc', 'total comp', 'offer', 'negotiate', 'bonus', 'stock']):
        categories.append('Compensation')
    
    if any(word in text_lower for word in ['amazon', 'google', 'meta', 'microsoft', 'faang', 'big tech', 'bigtech']):
        categories.append('Big Tech')
    
    if any(word in text_lower for word in ['course', 'class', 'degree', 'major', 'masters', 'msc', 'phd', 'university', 'college', 'bootcamp']):
        categories.append('Education')
    
    if any(word in text_lower for word in ['advice', 'help', 'question', 'should i', 'how to', 'tips', 'guidance', 'what should']):
        categories.append('Seeking Advice')
    
    if any(word in text_lower for word in ['reject', 'rejection', 'ghost', 'ghosted', 'fail', 'failed', 'struggle', 'difficult', 'hard', 'anxiety', 'depressed']):
        categories.append('Challenges')
    
    return categories if categories else ['General']

categorize_udf = udf(categorize_topic, ArrayType(StringType()))

df_categorized = df_filtered.withColumn(
    "topics",
    categorize_udf(col("full_text"))
)

# Explode topics for counting
df_topics = df_categorized.select(
    "id", "title", "score", "num_comments",
    explode(col("topics")).alias("topic")
)

topic_distribution = df_topics.groupBy("topic") \
    .agg(
        count("*").alias("post_count"),
        avg("score").alias("avg_score"),
        avg("num_comments").alias("avg_comments")
    ) \
    .orderBy(desc("post_count"))

print("\n Topic Distribution:")
topic_distribution.show(truncate=False)


# 6. Industry Distribution Analysis
print("\n[STEP 6] Analyzing industry distribution...")

df_industries = df_categorized.select(
    "id", "title", "score", "num_comments", "created_datetime",
    explode(col("industries")).alias("industry")
)

industry_distribution = df_industries.groupBy("industry") \
    .agg(
        count("*").alias("post_count"),
        avg("score").alias("avg_score"),
        avg("num_comments").alias("avg_comments")
    ) \
    .orderBy(desc("post_count"))

print("\n Industry Distribution:")
industry_distribution.show(truncate=False)

# Topic by industry cross-analysis
print("\nGenerating topic-industry cross-analysis...")

df_topic_exploded = df_categorized.select(
    "id",
    explode(col("topics")).alias("topic"),
    col("industries"),
    "score",
    "num_comments"
)

df_topic_industry = df_topic_exploded.select(
    "id",
    "topic",
    explode(col("industries")).alias("industry"),
    "score",
    "num_comments"
)

topic_by_industry = df_topic_industry.groupBy("industry", "topic") \
    .agg(
        count("*").alias("post_count"),
        avg("score").alias("avg_score")
    ) \
    .orderBy("industry", desc("post_count"))

print("\n Topics by Industry:")
topic_by_industry.show(50, truncate=False)


# 7. TF-IDF Keyword Analysis
print("\n[STEP 7] Running TF-IDF analysis...")

cv = CountVectorizer(inputCol="filtered_words", outputCol="raw_features", vocabSize=500)
cv_model = cv.fit(df_filtered)
df_cv = cv_model.transform(df_filtered)

idf = IDF(inputCol="raw_features", outputCol="features")
idf_model = idf.fit(df_cv)
df_tfidf = idf_model.transform(df_cv)

vocabulary = cv_model.vocabulary
print(f" Extracted {len(vocabulary)} unique terms")
print(f"  Sample vocabulary: {vocabulary[:20]}")


# 8. Engagement Analysis
print("\n[STEP 8] Analyzing post engagement patterns...")

engagement_by_topic = topic_distribution.select(
    col("topic"),
    col("avg_score").alias("avg_upvotes"),
    col("avg_comments"),
    col("post_count").alias("total_posts")
).orderBy(desc("avg_comments"))

print("\n Engagement by Topic:")
engagement_by_topic.show(truncate=False)

engagement_by_industry = industry_distribution.select(
    col("industry"),
    col("avg_score").alias("avg_upvotes"),
    col("avg_comments"),
    col("post_count").alias("total_posts")
).orderBy(desc("avg_comments"))

print("\n Engagement by Industry:")
engagement_by_industry.show(truncate=False)

engagement_by_hour = df_cleaned.groupBy("hour") \
    .agg(
        count("*").alias("post_count"),
        avg("score").alias("avg_score"),
        avg("num_comments").alias("avg_comments")
    )

all_hours_df = spark.createDataFrame(
    [(hour,) for hour in range(24)],
    ["hour"]
)

engagement_by_hour = all_hours_df.join(
    engagement_by_hour,
    on="hour",
    how="left"
).fillna(0, subset=["post_count", "avg_score", "avg_comments"]) \
 .orderBy("hour")

print("\n Engagement by Hour:")
engagement_by_hour.show(24, truncate=False)


# 9. Company Mention Analysis
print("\n[STEP 9] Extracting company mentions...")

companies_dict = {
    'amazon': 'Amazon',
    'google': 'Google',
    'meta': 'Meta',
    'facebook': 'Facebook',
    'microsoft': 'Microsoft',
    'apple': 'Apple',
    'netflix': 'Netflix',
    'uber': 'Uber',
    'lyft': 'Lyft',
    'airbnb': 'Airbnb',
    'salesforce': 'Salesforce',
    'oracle': 'Oracle',
    'ibm': 'IBM',
    'intel': 'Intel',
    'nvidia': 'NVIDIA',
    'tesla': 'Tesla',
    'spotify': 'Spotify',
    'twitter': 'Twitter',
    'palantir': 'Palantir',
    'stripe': 'Stripe',
    'databricks': 'Databricks',
    'snowflake': 'Snowflake',
    'citadel': 'Citadel',
    'jane street': 'Jane Street',
    'two sigma': 'Two Sigma',
    'goldman sachs': 'Goldman Sachs',
    'jp morgan': 'JP Morgan',
    'doordash': 'DoorDash',
    'instacart': 'Instacart'
}

def extract_companies(text):
    if not text:
        return []
    text_lower = text.lower()
    found = [companies_dict[company] for company in companies_dict.keys() 
             if company in text_lower]
    return found if found else []

extract_companies_udf = udf(extract_companies, ArrayType(StringType()))

df_companies = df_filtered.withColumn(
    "mentioned_companies",
    extract_companies_udf(col("full_text"))
).filter(size(col("mentioned_companies")) > 0)

df_company_mentions = df_companies.select(
    explode(col("mentioned_companies")).alias("company"),
    "score",
    "num_comments"
)

company_stats = df_company_mentions.groupBy("company") \
    .agg(
        count("*").alias("mention_count"),
        avg("score").alias("avg_score"),
        avg("num_comments").alias("avg_engagement")
    ) \
    .orderBy(desc("mention_count"))

print("\n Company Mentions:")
company_stats.show(truncate=False)


# 10. Advice Category Analysis
print("\n[STEP 10] Analyzing advice categories...")

# Add advice categories to posts
df_with_advice = df_categorized.withColumn(
    "advice_categories",
    detect_advice_udf(col("full_text"))
)

# Filter posts that contain advice
df_advice_posts = df_with_advice.filter(
    size(col("advice_categories")) > 0
)

advice_post_count = df_advice_posts.count()
total_posts = df_categorized.count()
advice_percentage = (advice_post_count / total_posts * 100) if total_posts > 0 else 0

print(f" Posts containing advice: {advice_post_count:,} ({advice_percentage:.1f}% of total)")

# Overall advice category distribution
df_advice_exploded = df_advice_posts.select(
    "id", "title", "score", "num_comments", "created_datetime",
    explode(col("advice_categories")).alias("advice_category"),
    col("industries")
)

overall_advice_distribution = df_advice_exploded.groupBy("advice_category") \
    .agg(
        count("*").alias("post_count"),
        avg("score").alias("avg_score"),
        avg("num_comments").alias("avg_engagement")
    ) \
    .orderBy(desc("post_count"))

print("\n Overall Advice Category Distribution:")
overall_advice_distribution.show(20, truncate=False)

# Advice categories by industry
print("\nAnalyzing advice categories by industry...")

df_advice_by_industry = df_advice_exploded.select(
    "id",
    "advice_category",
    explode(col("industries")).alias("industry"),
    "score",
    "num_comments"
)

advice_by_industry = df_advice_by_industry.groupBy("industry", "advice_category") \
    .agg(
        count("*").alias("post_count"),
        avg("score").alias("avg_score")
    ) \
    .orderBy("industry", desc("post_count"))

print("\n Advice Categories by Industry:")
advice_by_industry.show(50, truncate=False)

# Top advice category per industry
top_advice_per_industry = advice_by_industry \
    .withColumn("rank", row_number().over(
        Window.partitionBy("industry").orderBy(desc("post_count"))
    )) \
    .filter(col("rank") == 1) \
    .drop("rank")

print("\n Most Common Advice Category per Industry:")
top_advice_per_industry.show(truncate=False)

# Advice trends over time
df_advice_timeline = df_advice_exploded \
    .withColumn("year_month", date_format(col("created_datetime"), "yyyy-MM")) \
    .groupBy("year_month", "advice_category") \
    .agg(count("*").alias("post_count")) \
    .orderBy("year_month", "advice_category")

print("\n Advice Category Trends Over Time (sample):")
df_advice_timeline.show(30, truncate=False)


# 10. SENTIMENT ANALYSIS
print("\n[STEP 10] Analyzing sentiment indicators...")

def get_sentiment(text):
    if not text:
        return "Neutral"
    
    text_lower = text.lower()
    
    positive_words = ['success', 'offer', 'accepted', 'great', 'excited', 'happy', 
                      'amazing', 'awesome', 'love', 'best', 'good', 'congratulations',
                      'congratz', 'congrats', 'proud', 'thrilled', 'contract', 'full time', 
                      'full-time', 'fulltime', 'internship', 'intern', 'offered', 'secured', 
                      'landed', 'scored', 'won', 'positive', 'upvote', 'upvoted', 'smooth', 
                      'easy', 'relieved', 'grateful', 'fortunate', 'successful', 'successfully',
                      'overjoyed', 'ecstatic', 'elated', 'joyful', 'celebrate', 'celebrating', 'celebration']
    negative_words = ['reject', 'rejection', 'ghost', 'ghosted', 'fail', 'failed',
                      'sad', 'depressed', 'anxious', 'worried', 'stress', 'stressful',
                      'difficult', 'hard', 'no response', 'bad', 'terrible', 'awful', 
                      'negative', 'downvote', 'downvoted', 'struggle', 'struggling', 
                      'tough', 'rough', 'heartbreaking', 'disappointed', 'disappointing',
                      'frustrated', 'frustrating', 'overwhelmed', 'overwhelming', 'devastated', 
                      'miserable', 'hopeless', 'discouraged', 'disheartened', 'defeated', 'dismayed', 'despair', 'desperate', 'gloomy']
    
    pos_count = sum(1 for word in positive_words if word in text_lower)
    neg_count = sum(1 for word in negative_words if word in text_lower)
    
    if pos_count > neg_count:
        return "Positive"
    elif neg_count > pos_count:
        return "Negative"
    else:
        return "Neutral"

sentiment_udf = udf(get_sentiment, StringType())

df_sentiment = df_filtered.withColumn(
    "sentiment",
    sentiment_udf(col("full_text"))
)

sentiment_dist = df_sentiment.groupBy("sentiment") \
    .agg(
        count("*").alias("count"),
        avg("score").alias("avg_score"),
        avg("num_comments").alias("avg_comments")
    ) \
    .orderBy(desc("count"))

print("\n Sentiment Distribution:")
sentiment_dist.show(truncate=False)

df_sentiment_industry = df_sentiment.select(
    explode(col("industries")).alias("industry"),
    col("sentiment"),
    col("score"),
    col("num_comments")
)

sentiment_by_industry = df_sentiment_industry.groupBy("industry", "sentiment") \
    .agg(count("*").alias("count")) \
    .orderBy("industry", desc("count"))

print("\n Sentiment by Industry:")
sentiment_by_industry.show(50, truncate=False)


# 11. Time series data
print("\n[STEP 11] Creating time series data...")

posts_over_time = df_cleaned.groupBy("year", "month", "day") \
    .agg(
        count("*").alias("post_count"),
        avg("score").alias("avg_score"),
        avg("num_comments").alias("avg_comments")
    ) \
    .orderBy("year", "month", "day")

print("\n Posts Over Time:")
posts_over_time.show(20, truncate=False)

df_industry_time = df_categorized.select(
    "year", "month",
    explode(col("industries")).alias("industry")
)

industry_trends = df_industry_time.groupBy("year", "month", "industry") \
    .agg(count("*").alias("post_count")) \
    .orderBy("year", "month", "industry")

print("\n Industry Trends Over Time:")
industry_trends.show(20, truncate=False)

print("\n[STEP 11b] Creating additional analysis dataframes...")

# 1. sentiment_by_topic - group sentiment analysis by topics (not industries)
df_sentiment_categorized = df_categorized.withColumn(
    "sentiment",
    sentiment_udf(col("full_text"))
)

df_sentiment_topic = df_sentiment_categorized.select(
    explode(col("topics")).alias("topic"),
    col("sentiment"),
    col("score"),
    col("num_comments")
)

sentiment_by_topic = df_sentiment_topic.groupBy("topic", "sentiment") \
    .agg(count("*").alias("count")) \
    .orderBy("topic", desc("count"))

print(" Sentiment by Topic")

# 2. posts_by_industry - rename industry_distribution
posts_by_industry = industry_distribution.select(
    col("industry"),
    col("post_count"),
    col("avg_score").alias("avg_engagement_score"),
    col("avg_comments").alias("avg_comments_count")
)

print(" Posts by Industry")

# 3. salary_stats - extract salary mentions from text
def extract_salary(text):
    """Extract numerical salary values from text"""
    import re
    if not text:
        return []
    salary_pattern = r'\$?(\d{1,3}(?:,\d{3})*(?:\.\d{2})?|\d+k)'
    matches = re.findall(salary_pattern, text, re.IGNORECASE)
    return matches if matches else []

salary_udf = udf(extract_salary, ArrayType(StringType()))

df_with_salary = df_filtered.withColumn(
    "salary_mentions",
    salary_udf(col("full_text"))
).filter(size(col("salary_mentions")) > 0)

if df_with_salary.count() > 0:
    df_salary_exploded = df_with_salary.select(
        explode(col("industries")).alias("industry"),
        col("salary_mentions"),
        col("score"),
        col("num_comments")
    )
    
    salary_stats = df_salary_exploded.groupBy("industry") \
        .agg(
            count("*").alias("salary_mention_posts"),
            avg("score").alias("avg_engagement_score"),
            avg("num_comments").alias("avg_comments"),
            collect_list("salary_mentions").alias("salary_mentions_list")
        ) \
        .orderBy(desc("salary_mention_posts"))
else:
    # Create empty dataframe with correct schema if no salary mentions found
    salary_stats = spark.createDataFrame(
        [],
        ["industry", "salary_mention_posts", "avg_engagement_score", "avg_comments", "salary_mentions_list"]
    )

print(" Salary Statistics")

# 4. experience_distribution - extract experience level info
experience_keywords = {
    'No Experience': ['high school', 'high-school', 'freshman', 'sophomore', 'junior', 'senior', 'soon to graduate'],
    'New Grad': ['new grad', 'newgrad', 'recent grad', 'recent graduate', 'graduation'],
    '1-2 Years': ['1 year', '2 years', '1-2 years', 'entry level', 'entry-level', 'junior level'],
    '3-5 Years': ['3 years', '4 years', '5 years', '3-5 years', 'mid level', 'mid-level'],
    '5+ Years': ['5+ years', '5 years', '10 years', 'senior level', 'senior engineer', 'staff']
}

def extract_experience(text):
    if not text:
        return "Not Specified"
    text_lower = text.lower()
    for exp_level, keywords in experience_keywords.items():
        if any(keyword in text_lower for keyword in keywords):
            return exp_level
    return "Not Specified"

exp_udf = udf(extract_experience, StringType())

df_with_exp = df_filtered.withColumn(
    "experience_level",
    exp_udf(col("full_text"))
)

experience_distribution = df_with_exp.groupBy("experience_level") \
    .agg(count("*").alias("post_count")) \
    .orderBy(desc("post_count"))

print(" Experience Distribution")

# 5. skills_summary - extract technical skills
skills_keywords = [
    'python', 'java', 'javascript', 'c++', 'c#', 'go', 'rust', 'typescript',
    'sql', 'react', 'angular', 'vue', 'node', 'django', 'flask', 'spring',
    'aws', 'gcp', 'azure', 'kubernetes', 'docker', 'git', 'linux',
    'machine learning', 'ml', 'ai', 'deep learning', 'data science',
    'frontend', 'backend', 'fullstack', 'full stack', 'devops'
]

def extract_skills(text):
    if not text:
        return []
    text_lower = text.lower()
    found_skills = [skill for skill in skills_keywords if skill in text_lower]
    return list(set(found_skills))

skills_udf = udf(extract_skills, ArrayType(StringType()))

df_with_skills = df_filtered.withColumn(
    "mentioned_skills",
    skills_udf(col("full_text"))
).filter(size(col("mentioned_skills")) > 0)

if df_with_skills.count() > 0:
    df_skills_exploded = df_with_skills.select(
        explode(col("mentioned_skills")).alias("skill")
    )
    
    skills_summary = df_skills_exploded.groupBy("skill") \
        .agg(count("*").alias("skill_count")) \
        .orderBy(desc("skill_count"))
else:
    skills_summary = spark.createDataFrame(
        [("General Programming", 1)],
        ["skill", "skill_count"]
    )

print(" Skills Summary")

# skills_by_industry — top skills per industry
df_si = df_categorized.withColumn(
    "mentioned_skills", skills_udf(col("full_text"))
).filter(size(col("mentioned_skills")) > 0).select(
    "id",
    explode(col("industries")).alias("industry"),
    col("mentioned_skills")
)

df_si = df_si.select(
    "id", "industry",
    explode(col("mentioned_skills")).alias("skill")
)

skills_by_industry = df_si.groupBy("industry", "skill") \
    .agg(count("*").alias("skill_count")) \
    .orderBy("industry", desc("skill_count"))

print(" Skills by Industry")

# 6. temporal_trends - aggregate time series by month
df_with_sentiment = df_sentiment.withColumn(
    "year_month",
    date_format(col("created_datetime"), "yyyy-MM")
)

temporal_trends = df_with_sentiment.groupBy("year_month", "sentiment") \
    .agg(
        count("*").alias("post_count"),
        avg("score").alias("avg_score"),
        avg("num_comments").alias("avg_comments")
    ) \
    .orderBy("year_month", desc("post_count"))

print(" Temporal Trends")

# 7. network_metrics - create engagement network summary
total_posts = df_cleaned.count()
total_comments_result = df_cleaned.agg(spark_sum("num_comments")).collect()[0][0]
avg_score_result = df_cleaned.agg(avg("score")).collect()[0][0]
avg_comments_result = df_cleaned.agg(avg("num_comments")).collect()[0][0]

network_metrics = spark.createDataFrame([
    ("Total Posts", float(total_posts)),
    ("Total Comments", float(total_comments_result if total_comments_result else 0)),
    ("Avg Score", float(avg_score_result) if avg_score_result else 0.0),
    ("Avg Comments", float(avg_comments_result) if avg_comments_result else 0.0)
], ["metric", "value"])

print(" Network Metrics")


# 12. Upload processed data directly to S3
print(f"\n[STEP 12] Uploading processed data to s3://{S3_BUCKET}/processed/{DATE_STR}/")

try:
    # 1. Topic Analysis
    upload_csv_to_s3(topic_distribution, DATE_STR, "topic_analysis")

    # 2. Sentiment by Topic
    upload_csv_to_s3(sentiment_by_topic, DATE_STR, "sentiment_by_topic")

    # 3. Posts by Industry
    upload_csv_to_s3(posts_by_industry, DATE_STR, "posts_by_industry")

    # 4. Salary Statistics — parse salary strings to numeric before uploading
    try:
        sal_count = salary_stats.count()
        print(f"\n salary_stats: {sal_count} rows")
        if sal_count > 0:
            sal_pandas = salary_stats.toPandas()

            def _parse_sal(s):
                s = str(s).strip().lower().replace('$', '').replace(',', '').replace(' ', '')
                if s.endswith('k'):
                    try:
                        v = float(s[:-1]) * 1000
                        return v if 30000 <= v <= 1000000 else None
                    except Exception:
                        return None
                try:
                    v = float(s)
                    return v if 30000 <= v <= 1000000 else None
                except Exception:
                    return None

            def _sal_stats(mentions_list):
                vals = []
                for item in (mentions_list or []):
                    for s in (item if isinstance(item, list) else [item]):
                        v = _parse_sal(s)
                        if v is not None:
                            vals.append(v)
                if not vals:
                    return None, None, None
                vals.sort()
                return vals[len(vals) // 2], vals[0], vals[-1]

            tuples = sal_pandas['salary_mentions_list'].apply(_sal_stats)
            sal_pandas['median_salary'] = tuples.apply(lambda x: x[0])
            sal_pandas['min_salary'] = tuples.apply(lambda x: x[1])
            sal_pandas['max_salary'] = tuples.apply(lambda x: x[2])
            sal_pandas = sal_pandas.drop(columns=['salary_mentions_list'])
            upload_pandas_to_s3(sal_pandas, DATE_STR, "salary_stats")
        else:
            print(f"  WARNING: salary_stats is empty! Skipping.")
    except Exception as e:
        print(f"  ERROR processing salary_stats: {str(e)}")
        import traceback
        traceback.print_exc()

    # 5. Experience Distribution
    upload_csv_to_s3(experience_distribution, DATE_STR, "experience_distribution")

    # 6. Skills Summary
    upload_csv_to_s3(skills_summary, DATE_STR, "skills_summary")

    # 7. Temporal Trends
    upload_csv_to_s3(temporal_trends, DATE_STR, "temporal_trends")

    # 8. Network Metrics
    upload_csv_to_s3(network_metrics, DATE_STR, "network_metrics")

    # 9. Company Mentions
    upload_csv_to_s3(company_stats, DATE_STR, "company_mentions")

    # 10. Topic by Industry
    upload_csv_to_s3(topic_by_industry, DATE_STR, "topic_by_industry")

    # 11. Skills by Industry
    upload_csv_to_s3(skills_by_industry, DATE_STR, "skills_by_industry")

    print("\n" + "="*60)
    print(" ALL DATASETS UPLOADED SUCCESSFULLY!")
    print("="*60)
    print(f"\nS3 location: s3://{S3_BUCKET}/processed/{DATE_STR}/")
    print("\nDatasets uploaded:")
    for i, name in enumerate([
        "topic_analysis", "sentiment_by_topic", "posts_by_industry",
        "salary_stats", "experience_distribution", "skills_summary",
        "temporal_trends", "network_metrics", "company_mentions",
        "topic_by_industry", "skills_by_industry"
    ], 1):
        print(f"  {i:2}. {name}/")

except Exception as e:
    print(f"\n FATAL ERROR during upload process:")
    print(str(e))
    import traceback
    traceback.print_exc()


# 13. Summary statistics and final output
print("\n" + "="*60)
print("SUMMARY STATISTICS")
print("="*60)

print("\n1. Topic Distribution:")
topic_distribution.show(10, truncate=False)

print("\n2. Sentiment by Topic:")
sentiment_by_topic.show(10, truncate=False)

print("\n3. Top Industries by Post Count:")
posts_by_industry.orderBy(desc("post_count")).show(10, truncate=False)

print("\n4. Salary Statistics by Industry:")
salary_stats.orderBy(desc("salary_mention_posts")).show(10, truncate=False)

print("\n5. Experience Level Distribution:")
experience_distribution.orderBy(desc("post_count")).show(10, truncate=False)

print("\n6. Top Skills:")
skills_summary.orderBy(desc("skill_count")).show(20, truncate=False)

print("\n7. Temporal Trends (Recent Months):")
temporal_trends.orderBy(desc("year_month")).show(12, truncate=False)

print("\n8. Network Metrics:")
network_metrics.show(10, truncate=False)

print("\n" + "="*60)
print("PROCESSING COMPLETE!")
print("="*60)

# Stop Spark session
spark.stop()
print("\n Spark session stopped. Analysis complete!")


