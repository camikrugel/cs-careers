# CS Career Intelligence from Reddit

Computer science students frequently rely on Reddit communities such as r/csMajors and r/cscareerquestions for advice on internships, recruiting strategies, course selection, and the broader tech job market. These discussions contain valuable insights from students and professionals, but the information is scattered across thousands of posts and comment threads, making it difficult to identify overall trends or actionable advice.

This project aims to build a big data pipeline that collects and analyzes Reddit discussions related to CS careers and internship preparation. The system will automatically ingest posts and comments from relevant subreddits, process the text using PySpark, and extract structured insights about common topics and trends in career discussions.

While some categories of discussion are expected (such as internships, interviews, or course selection), the analysis will not rely solely on predefined labels. Instead, the pipeline will detect recurring themes directly from the data, meaning that some insights shown in the final results may vary depending on patterns present in the collected dataset.

To simulate a continuously updating system, the pipeline will periodically retrieve new Reddit posts through the Reddit API, allowing the analysis to update regularly in a near real-time manner.

## Data Storage & Access (AWS S3)
The project data is managed in the following AWS S3 bucket: `bigdata-career-project`.

### Current Access Logic
To ensure seamless collaboration and pipeline automation in the lab environment, the bucket is currently configured with **Full Public Access** (List, Get, and Put).

- **Block Public Access:** Disabled.
- **Bucket Policy:** Configured to allow `s3:GetObject`, `s3:ListBucket`, and `s3:PutObject` for all principals (`*`).
  
> **⚠️ Note:** This configuration allows any contributor to upload processed data or logs directly to the bucket. While this simplifies the workflow for a lab setting, it should be restricted to specific IAM ARNs for production environments.

# Setup
# Install required packages
# Make sure to create new environment with python = 3.10
pip install -r data-processing/requirements.txt

# Download NLTK Data
python -c "import nltk; nltk.download('stopwords'); nltk.download('punkt')"
