 Data Processing Pipeline

## Overview
This week implements the core PySpark processing pipeline for Reddit CS career data analysis.

## Components

### 1. Text Cleaning (`clean_text_pipeline`)
- Removes URLs and special characters
- Normalizes text to lowercase
- Removes Reddit markdown
- Eliminates stopwords
- **Output**: `cleaned_title`, `cleaned_body`, `processed_text`

### 2. Topic Classification (`classify_topics`)
- Categories: internship, interview, salary, job_search, resume, projects, courses, new_grad, faang
- Uses regex pattern matching
- **Output**: `primary_topic`, topic flag columns

### 3. Salary Extraction (`extract_salaries`)
- Patterns: $XXk, $XXX,XXX, "tc: 120k"
- Validates range (30k-500k)
- **Output**: `mentioned_salary` (float)

### 4. Sentiment Analysis (`apply_sentiment_analysis`)
- Uses TextBlob library
- **Output**: `sentiment_polarity`, `sentiment_subjectivity`, `sentiment_category`

### 5. Skill Extraction (`extract_skills_keywords`)
- Detects mentions of programming languages, frameworks, tools
- **Output**: Binary columns `skill_*`

## Running the Pipeline

```bash
# Set AWS credentials
export AWS_ACCESS_KEY_ID="your_key"
export AWS_SECRET_ACCESS_KEY="your_secret"

# Run main pipeline
python process_reddit_data.py

# Generate analytics
python generate_analytics.py