from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_replace, lower, trim, udf, 
    when, lit, explode, split, count, to_date
)
from pyspark.sql.types import StringType, FloatType, ArrayType, StructType, StructField
import re
from textblob import TextBlob
import nltk
from nltk.corpus import stopwords
import boto3
from datetime import datetime
import config

# Initialize Spark Session
def create_spark_session():
    """Create and configure Spark session with S3 access"""
    spark = SparkSession.builder \
        .appName(config.SPARK_APP_NAME) \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.access.key", config.AWS_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", config.AWS_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ============================================
# TEXT CLEANING AND NORMALIZATION
# ============================================

def clean_text_pipeline(df):
    """
    Comprehensive text cleaning pipeline
    - Remove URLs, special characters
    - Convert to lowercase
    - Remove extra whitespace
    - Remove Reddit-specific formatting
    """
    print("Starting text cleaning pipeline...")
    
    # Remove URLs (http, https, www)
    df = df.withColumn(
        "cleaned_title",
        regexp_replace(col("title"), r"http\S+|www\.\S+", "")
    )
    df = df.withColumn(
        "cleaned_body",
        regexp_replace(col("selftext"), r"http\S+|www\.\S+", "")
    )
    
    # Remove Reddit-specific markdown
    df = df.withColumn(
        "cleaned_title",
        regexp_replace(col("cleaned_title"), r"\[.*?\]|\(.*?\)", "")
    )
    df = df.withColumn(
        "cleaned_body",
        regexp_replace(col("cleaned_body"), r"\[.*?\]|\(.*?\)", "")
    )
    
    # Remove special characters (keep letters, numbers, spaces)
    df = df.withColumn(
        "cleaned_title",
        regexp_replace(col("cleaned_title"), r"[^a-zA-Z0-9\s]", " ")
    )
    df = df.withColumn(
        "cleaned_body",
        regexp_replace(col("cleaned_body"), r"[^a-zA-Z0-9\s]", " ")
    )
    
    # Convert to lowercase
    df = df.withColumn("cleaned_title", lower(col("cleaned_title")))
    df = df.withColumn("cleaned_body", lower(col("cleaned_body")))
    
    # Remove extra whitespace
    df = df.withColumn("cleaned_title", trim(regexp_replace(col("cleaned_title"), r"\s+", " ")))
    df = df.withColumn("cleaned_body", trim(regexp_replace(col("cleaned_body"), r"\s+", " ")))
    
    # Create combined text for analysis
    df = df.withColumn(
        "combined_text",
        when(col("cleaned_body").isNotNull() & (col("cleaned_body") != ""),
             concat(col("cleaned_title"), lit(" "), col("cleaned_body")))
        .otherwise(col("cleaned_title"))
    )
    
    print(f"Text cleaning complete. Processed {df.count()} posts.")
    return df


def remove_stopwords_udf():
    """UDF to remove stopwords from text"""
    stop_words = set(stopwords.words('english'))
    
    def remove_stopwords(text):
        if not text:
            return ""
        words = text.split()
        filtered_words = [word for word in words if word not in stop_words]
        return " ".join(filtered_words)
    
    return udf(remove_stopwords, StringType())


def apply_stopword_removal(df):
    """Apply stopword removal to cleaned text"""
    print("Removing stopwords...")
    remove_stopwords = remove_stopwords_udf()
    
    df = df.withColumn("processed_text", remove_stopwords(col("combined_text")))
    
    print("Stopword removal complete.")
    return df


# ============================================
# TOPIC CLASSIFICATION
# ============================================

def classify_topics(df):
    """
    Classify posts into predefined career-related topics
    Topics: internship, job_search, interview, salary, courses, projects, resume
    """
    print("Starting topic classification...")
    
    # Define keyword patterns for each topic
    topic_patterns = {
        'internship': r'\b(intern|internship|co-op|coop|summer position)\b',
        'job_search': r'\b(job|career|offer|employment|hiring|recruiter|application)\b',
        'interview': r'\b(interview|leetcode|coding challenge|technical interview|behavioral|oa|online assessment)\b',
        'salary': r'\b(salary|compensation|pay|tc|total comp|stock|rsu|bonus)\b',
        'courses': r'\b(course|class|cs\d+|algorithm|data structure|operating system|database)\b',
        'projects': r'\b(project|portfolio|github|side project|personal project)\b',
        'resume': r'\b(resume|cv|curriculum vitae)\b',
        'new_grad': r'\b(new grad|new-grad|entry level|junior)\b',
        'faang': r'\b(faang|manga|big tech|google|amazon|meta|facebook|apple|netflix|microsoft)\b'
    }
    
    # Apply pattern matching for each topic
    for topic, pattern in topic_patterns.items():
        df = df.withColumn(
            f"topic_{topic}",
            when(
                col("combined_text").rlike(pattern), 1
            ).otherwise(0)
        )
    
    # Create primary topic (first match, prioritized)
    df = df.withColumn(
        "primary_topic",
        when(col("topic_internship") == 1, "internship")
        .when(col("topic_interview") == 1, "interview")
        .when(col("topic_salary") == 1, "salary")
        .when(col("topic_job_search") == 1, "job_search")
        .when(col("topic_new_grad") == 1, "new_grad")
        .when(col("topic_resume") == 1, "resume")
        .when(col("topic_projects") == 1, "projects")
        .when(col("topic_courses") == 1, "courses")
        .when(col("topic_faang") == 1, "faang")
        .otherwise("general")
    )
    
    # Count total topics per post
    topic_cols = [f"topic_{t}" for t in topic_patterns.keys()]
    df = df.withColumn("topic_count", sum([col(c) for c in topic_cols]))
    
    print("Topic classification complete.")
    print("Topic distribution:")
    df.groupBy("primary_topic").count().orderBy(col("count").desc()).show()
    
    return df


# ============================================
# SALARY EXTRACTION
# ============================================

def extract_salaries_udf():
    """
    UDF to extract salary mentions from text
    Patterns: $XXk, $XXX,XXX, XXk, etc.
    """
    salary_patterns = [
        r'\$(\d{1,3})[kK]',                    # $120k
        r'\$(\d{1,3}),?(\d{3})',               # $120,000 or $120000
        r'(\d{1,3})[kK]\s*(?:salary|comp)',    # 120k salary
        r'tc[:\s]*\$?(\d{1,3})[kK]',           # tc: 120k
    ]
    
    def extract_salary(text):
        if not text:
            return None
        
        salaries = []
        for pattern in salary_patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                if 'k' in match.group().lower() or 'K' in match.group():
                    # Extract number before 'k'
                    num = re.search(r'(\d{1,3})', match.group())
                    if num:
                        salary = int(num.group(1)) * 1000
                        if 30000 <= salary <= 500000:  # Reasonable range
                            salaries.append(salary)
                else:
                    # Extract full number
                    nums = re.findall(r'\d+', match.group())
                    if nums:
                        salary = int(''.join(nums))
                        if 30000 <= salary <= 500000:
                            salaries.append(salary)
        
        return float(max(salaries)) if salaries else None
    
    return udf(extract_salary, FloatType())


def apply_salary_extraction(df):
    """Extract salary mentions from posts"""
    print("Extracting salary information...")
    
    extract_salary = extract_salaries_udf()
    df = df.withColumn("mentioned_salary", extract_salary(col("combined_text")))
    
    salary_count = df.filter(col("mentioned_salary").isNotNull()).count()
    print(f"Found {salary_count} posts with salary mentions.")
    
    return df


# ============================================
# SENTIMENT ANALYSIS
# ============================================

def sentiment_analysis_udf():
    """
    UDF for sentiment analysis using TextBlob
    Returns: polarity (-1 to 1), subjectivity (0 to 1), and category
    """
    def analyze_sentiment(text):
        if not text or len(text.strip()) == 0:
            return (0.0, 0.0, "neutral")
        
        try:
            blob = TextBlob(text)
            polarity = blob.sentiment.polarity
            subjectivity = blob.sentiment.subjectivity
            
            # Categorize sentiment
            if polarity > 0.1:
                category = "positive"
            elif polarity < -0.1:
                category = "negative"
            else:
                category = "neutral"
            
            return (float(polarity), float(subjectivity), category)
        except:
            return (0.0, 0.0, "neutral")
    
    return udf(analyze_sentiment, StructType([
        StructField("polarity", FloatType()),
        StructField("subjectivity", FloatType()),
        StructField("category", StringType())
    ]))


def apply_sentiment_analysis(df):
    """Apply sentiment analysis to posts"""
    print("Performing sentiment analysis...")
    
    analyze_sentiment = sentiment_analysis_udf()
    df = df.withColumn("sentiment", analyze_sentiment(col("combined_text")))
    
    # Extract sentiment fields
    df = df.withColumn("sentiment_polarity", col("sentiment.polarity"))
    df = df.withColumn("sentiment_subjectivity", col("sentiment.subjectivity"))
    df = df.withColumn("sentiment_category", col("sentiment.category"))
    
    # Drop intermediate column
    df = df.drop("sentiment")
    
    print("Sentiment analysis complete.")
    df.groupBy("sentiment_category").count().show()
    
    return df


# ============================================
# KEYWORD AND SKILL EXTRACTION
# ============================================

def extract_skills_keywords(df):
    """Extract frequently mentioned skills and technologies"""
    print("Extracting skills and keywords...")
    
    # Define common CS skills and technologies
    skills_keywords = [
        'python', 'java', 'javascript', 'typescript', 'c\\+\\+', 'cpp',
        'react', 'angular', 'vue', 'node', 'express',
        'sql', 'nosql', 'mongodb', 'postgres', 'mysql',
        'aws', 'azure', 'gcp', 'docker', 'kubernetes',
        'machine learning', 'ml', 'ai', 'deep learning',
        'data structures', 'algorithms', 'leetcode',
        'git', 'github', 'agile', 'scrum',
        'api', 'rest', 'graphql',
        'linux', 'unix', 'bash'
    ]
    
    # Create binary columns for each skill
    for skill in skills_keywords:
        skill_col_name = f"skill_{skill.replace(' ', '_').replace('\\\\', '')}"
        df = df.withColumn(
            skill_col_name,
            when(col("combined_text").rlike(rf'\b{skill}\b'), 1).otherwise(0)
        )
    
    print("Skill extraction complete.")
    return df


# ============================================
# MAIN PROCESSING PIPELINE
# ============================================

def process_reddit_data():
    """Main function to orchestrate the entire processing pipeline"""
    
    print("="*60)
    print("REDDIT CS CAREER DATA PROCESSING PIPELINE")
    print("="*60)
    
    # 1. Create Spark session
    spark = create_spark_session()
    
    # 2. Load raw data from S3
    print("\n[STEP 1] Loading raw data from S3...")
    s3_input_path = f"s3a://{config.S3_BUCKET}/{config.S3_RAW_DATA_PATH}*.json"
    
    try:
        df = spark.read.json(s3_input_path)
        print(f"Successfully loaded {df.count()} posts from S3")
        print(f"Schema: {df.columns}")
    except Exception as e:
        print(f"Error loading data: {e}")
        return
    
    # 3. Text cleaning and normalization
    print("\n[STEP 2] Text Cleaning and Normalization...")
    df = clean_text_pipeline(df)
    df = apply_stopword_removal(df)
    
    # 4. Topic classification
    print("\n[STEP 3] Topic Classification...")
    df = classify_topics(df)
    
    # 5. Salary extraction
    print("\n[STEP 4] Salary Extraction...")
    df = apply_salary_extraction(df)
    
    # 6. Sentiment analysis
    print("\n[STEP 5] Sentiment Analysis...")
    df = apply_sentiment_analysis(df)
    
    # 7. Skill extraction
    print("\n[STEP 6] Skill and Keyword Extraction...")
    df = extract_skills_keywords(df)
    
    # 8. Add processing metadata
    df = df.withColumn("processed_date", lit(datetime.now().strftime("%Y-%m-%d")))
    df = df.withColumn("post_date", to_date(col("created_utc").cast("timestamp")))
    
    # 9. Select final columns
    final_columns = [
        'id', 'title', 'selftext', 'author', 'subreddit', 
        'score', 'num_comments', 'created_utc', 'post_date',
        'cleaned_title', 'cleaned_body', 'combined_text', 'processed_text',
        'primary_topic', 'topic_count',
        'mentioned_salary',
        'sentiment_polarity', 'sentiment_subjectivity', 'sentiment_category',
        'processed_date'
    ]
    
    # Add topic flags
    topic_cols = [c for c in df.columns if c.startswith('topic_')]
    skill_cols = [c for c in df.columns if c.startswith('skill_')]
    final_columns.extend(topic_cols)
    final_columns.extend(skill_cols)
    
    df_final = df.select(*final_columns)
    
    # 10. Save to Parquet format in S3
    print("\n[STEP 7] Saving processed data to S3...")
    s3_output_path = f"s3a://{config.S3_BUCKET}/{config.S3_PROCESSED_DATA_PATH}processed_posts.parquet"
    
    df_final.write \
        .mode("overwrite") \
        .partitionBy("post_date") \
        .parquet(s3_output_path)
    
    print(f"Successfully saved processed data to {s3_output_path}")
    
    # 11. Generate summary statistics
    print("\n" + "="*60)
    print("PROCESSING SUMMARY")
    print("="*60)
    print(f"Total posts processed: {df_final.count()}")
    print(f"\nTopic Distribution:")
    df_final.groupBy("primary_topic").count().orderBy(col("count").desc()).show()
    print(f"\nSentiment Distribution:")
    df_final.groupBy("sentiment_category").count().show()
    print(f"\nPosts with salary mentions: {df_final.filter(col('mentioned_salary').isNotNull()).count()}")
    
    # Stop Spark session
    spark.stop()
    print("\nPipeline execution complete!")


if __name__ == "__main__":
    process_reddit_data()