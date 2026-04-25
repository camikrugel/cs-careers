# Data Schema

---

## Input Data

**Source:** Reddit API (`reddit.com/r/{subreddit}/{listing}.json`)  
**Location:** `s3://bigdata-cs-careers/raw/YYYY-MM-DD/posts.json`  
**Format:** JSON array of post objects

Each element in the array is one Reddit post with these fields:

| Field | Type | Description |
|---|---|---|
| id | string | Reddit post ID (unique) |
| title | string | Post title |
| selftext | string | Post body text (empty string if link-only post) |
| score | integer | Net upvotes (upvotes minus downvotes) |
| created_utc | float | Unix timestamp of when post was created |
| num_comments | integer | Number of comments on the post |
| url | string | Post URL (permalink or external link) |
| subreddit | string | `csMajors` or `cscareerquestions` |

**Example record:**
```json
{
  "id": "abc123",
  "title": "Just got my first SWE offer from Google!",
  "selftext": "Applied last October, went through 5 rounds...",
  "score": 847,
  "created_utc": 1713312000.0,
  "num_comments": 142,
  "url": "https://www.reddit.com/r/csMajors/comments/abc123/",
  "subreddit": "csMajors"
}
```

**Collection details:**
- Subreddits: `r/csMajors`, `r/cscareerquestions`
- Listings: `new` and `top` (both per subreddit)
- Up to 1000 posts per subreddit/listing combination (10 pages × 100 posts)
- Deduplicated by post `id` — each post appears once even if it appears in both `new` and `top`
- Accompanied by `metadata.json` with collection timestamp and post count

---

## Output Data

All 11 CSV datasets are written directly to `s3://bigdata-cs-careers/processed/YYYY-MM-DD/`. Each dataset lives in its own subfolder with a matching filename (e.g. `topic_analysis/topic_analysis.csv`).

---

## 1. topic_analysis

Post counts and engagement aggregated by topic category.

| Column | Type | Description |
|---|---|---|
| topic | string | Topic category |
| post_count | integer | Number of posts in this topic |
| avg_score | float | Average Reddit upvote score |
| avg_comments | float | Average number of comments |

**Topic categories:** Internship, Job Search, Interview Prep, Resume/Profile, Compensation, Big Tech, Education, Seeking Advice, Challenges, General

---

## 2. sentiment_by_topic

Sentiment breakdown per topic 

| Column | Type | Description |
|---|---|---|
| topic | string | Topic category |
| sentiment | string | Positive, Negative, or Neutral |
| count | integer | Number of posts with this sentiment in this topic |

**Sentiment method:** keyword-based (positive/negative word lists applied to combined title + selftext). Posts where positive word count > negative → Positive; negative > positive → Negative; otherwise → Neutral.

---

## 3. posts_by_industry

Post volume and engagement by industry, used for the horizontal bar chart.

| Column | Type | Description |
|---|---|---|
| industry | string | Industry category |
| post_count | integer | Number of posts mentioning this industry |
| avg_engagement_score | float | Average Reddit upvote score |
| avg_comments_count | float | Average number of comments |

**Industry categories:** FAANG/Big Tech, Unicorn Startup, Established Tech, Enterprise/Legacy, Hardware/Semiconductor, FinTech/Finance, Defense/Government, Early Stage Startup, Consulting/Services, E-commerce/Retail, Gaming/Entertainment, Healthcare/Biotech, General/Unspecified

Posts can belong to multiple industries (multi-label).

---

## 4. salary_stats

Salary mention analysis by industry — used for the salary table in the dashboard.

| Column | Type | Description |
|---|---|---|
| industry | string | Industry category |
| salary_mention_posts | integer | Number of posts containing salary patterns |
| avg_engagement_score | float | Average upvote score of salary-mentioning posts |
| avg_comments | float | Average comments of salary-mentioning posts |
| median_salary | float | Median parsed salary value in dollars (null if no parseable values) |
| min_salary | float | Minimum parsed salary value (null if no parseable values) |
| max_salary | float | Maximum parsed salary value (null if no parseable values) |

**Salary patterns matched:** `$XXX,XXX`, `XXXk`, `$XXXk` (annual); `$XX/hr`, `$XX/hour`, `$XX per hour`, `$XX an hour` (hourly). Hourly rates are annualized at 40 hrs/week × 52 weeks (×2080) before storing. Posts with no matches are excluded. Parsed values must be in the range $30,000–$1,000,000 to filter out non-salary numbers. The raw `salary_mentions_list` column is dropped before saving.

---

## 5. experience_distribution

Experience level breakdown used for the pie chart.

| Column | Type | Description |
|---|---|---|
| experience_level | string | Experience category |
| post_count | integer | Number of posts at this level |

**Experience levels:** No Experience, New Grad, 1-2 Years, 3-5 Years, 5+ Years, Not Specified

Detection is keyword-based (e.g. "new grad", "2 years", "senior engineer").

---

## 6. skills_summary

Most mentioned technical skills

| Column | Type | Description |
|---|---|---|
| skill | string | Skill or technology name |
| skill_count | integer | Number of posts mentioning this skill |

**Skills tracked:** python, java, javascript, c++, c#, go, rust, typescript, sql, react, angular, vue, node, django, flask, spring, aws, gcp, azure, kubernetes, docker, git, linux, machine learning, ml, ai, deep learning, data science, frontend, backend, fullstack, devops

---

## 7. temporal_trends

Monthly post volume by sentiment 
| Column | Type | Description |
|---|---|---|
| year_month | string | Month in `YYYY-MM` format |
| sentiment | string | Positive, Negative, or Neutral |
| post_count | integer | Number of posts that month with this sentiment |
| avg_score | float | Average upvote score |
| avg_comments | float | Average comment count |

---

## 8. network_metrics

Overall dataset statistics 

| Column | Type | Description |
|---|---|---|
| metric | string | Metric name |
| value | float | Metric value |

**Rows included:**

| metric | description |
|---|---|
| Total Posts | Total posts after cleaning |
| Total Comments | Sum of num_comments across all posts |
| Avg Score | Average Reddit upvote score |
| Avg Comments | Average comments per post |

---

## 9. company_mentions

Top company mentions by post volume — powers the "Top Company Mentions" bar chart.

| Column | Type | Description |
|---|---|---|
| company | string | Company display name (e.g. "Google", "Amazon") |
| mention_count | integer | Number of posts mentioning this company |
| avg_score | float | Average upvote score of posts mentioning this company |
| avg_engagement | float | Average comment count of posts mentioning this company |

**Companies tracked:** Amazon, Google, Meta, Microsoft, Apple, Netflix, Uber, Lyft, Airbnb, Salesforce, Oracle, IBM, Intel, NVIDIA, Tesla, Spotify, Twitter, Palantir, Stripe, Databricks, Snowflake, Citadel, Jane Street, Two Sigma, Goldman Sachs, JP Morgan, DoorDash, Instacart (29 total).

---

## 10. topic_by_industry

Cross-analysis of topics and industries — powers the "Topics by Industry" grouped bar chart.

| Column | Type | Description |
|---|---|---|
| industry | string | Industry category |
| topic | string | Topic category |
| post_count | integer | Number of posts in this industry+topic combination |
| avg_score | float | Average upvote score |

Posts can belong to multiple industries and topics (multi-label), so counts overlap.

---

## 11. skills_by_industry

Top technical skills per industry — powers the "Skills by Industry" dropdown chart.

| Column | Type | Description |
|---|---|---|
| industry | string | Industry category |
| skill | string | Skill or technology name |
| skill_count | integer | Number of posts in this industry mentioning this skill |

A post mentioning both FAANG and Python contributes 1 count to `(FAANG/Big Tech, python)`. Posts can appear in multiple industry rows (multi-label).

---

## File Locations

```
s3://bigdata-cs-careers/processed/YYYY-MM-DD/
├── topic_analysis/topic_analysis.csv
├── sentiment_by_topic/sentiment_by_topic.csv
├── posts_by_industry/posts_by_industry.csv
├── salary_stats/salary_stats.csv
├── experience_distribution/experience_distribution.csv
├── skills_summary/skills_summary.csv
├── temporal_trends/temporal_trends.csv
├── network_metrics/network_metrics.csv
├── company_mentions/company_mentions.csv
├── topic_by_industry/topic_by_industry.csv
└── skills_by_industry/skills_by_industry.csv
```
