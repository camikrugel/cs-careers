from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum, desc, month, year, dayofmonth
import config

def generate_analytics():
    """Generate aggregated analytics datasets for dashboard"""
    
    spark = SparkSession.builder \
        .appName("Reddit Analytics Generation") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.access.key", config.AWS_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", config.AWS_SECRET_KEY) \
        .getOrCreate()
    
    # Load processed data
    s3_input = f"s3a://{config.S3_BUCKET}/{config.S3_PROCESSED_DATA_PATH}processed_posts.parquet"
    df = spark.read.parquet(s3_input)
    
    # 1. Topic distribution over time
    topic_trends = df.groupBy("post_date", "primary_topic") \
        .agg(count("*").alias("post_count")) \
        .orderBy("post_date", desc("post_count"))
    
    topic_trends.write.mode("overwrite").parquet(
        f"s3a://{config.S3_BUCKET}/{config.S3_PROCESSED_DATA_PATH}analytics/topic_trends.parquet"
    )
    
    # 2. Salary statistics by topic
    salary_stats = df.filter(col("mentioned_salary").isNotNull()) \
        .groupBy("primary_topic") \
        .agg(
            count("*").alias("mention_count"),
            avg("mentioned_salary").alias("avg_salary"),
            min("mentioned_salary").alias("min_salary"),
            max("mentioned_salary").alias("max_salary")
        )
    
    salary_stats.write.mode("overwrite").parquet(
        f"s3a://{config.S3_BUCKET}/{config.S3_PROCESSED_DATA_PATH}analytics/salary_stats.parquet"
    )
    
    # 3. Sentiment trends over time
    sentiment_trends = df.groupBy("post_date", "sentiment_category") \
        .agg(
            count("*").alias("post_count"),
            avg("sentiment_polarity").alias("avg_polarity")
        ) \
        .orderBy("post_date")
    
    sentiment_trends.write.mode("overwrite").parquet(
        f"s3a://{config.S3_BUCKET}/{config.S3_PROCESSED_DATA_PATH}analytics/sentiment_trends.parquet"
    )
    
    # 4. Top skills mentioned
    skill_cols = [c for c in df.columns if c.startswith('skill_')]
    skill_counts = []
    
    for skill_col in skill_cols:
        skill_name = skill_col.replace('skill_', '')
        count_val = df.filter(col(skill_col) == 1).count()
        skill_counts.append((skill_name, count_val))
    
    skill_df = spark.createDataFrame(skill_counts, ["skill", "mention_count"])
    skill_df = skill_df.orderBy(desc("mention_count"))
    
    skill_df.write.mode("overwrite").parquet(
        f"s3a://{config.S3_BUCKET}/{config.S3_PROCESSED_DATA_PATH}analytics/skill_mentions.parquet"
    )
    
    print("Analytics generation complete!")
    spark.stop()

if __name__ == "__main__":
    generate_analytics()