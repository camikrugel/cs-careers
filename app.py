import streamlit as st
import pandas as pd
import plotly.express as px
import s3fs

st.set_page_config(page_title="Reddit CS Career Intelligence", layout="wide")

S3_BUCKET = "bigdata-cs-careers"

# Credentials from Streamlit secrets (set in app dashboard or .streamlit/secrets.toml locally)
fs = s3fs.S3FileSystem(
    key=st.secrets["aws"]["access_key_id"],
    secret=st.secrets["aws"]["secret_access_key"],
    token=st.secrets["aws"]["session_token"],
)


@st.cache_data(ttl=3600)
def detect_latest_processed_date():
    """Return the most recent YYYY-MM-DD folder under processed/ in S3."""
    prefix = f"{S3_BUCKET}/processed/"
    try:
        folders = fs.ls(prefix)
        dates = sorted([f.split("/")[-1] for f in folders if f.split("/")[-1]])
        return dates[-1] if dates else None
    except Exception:
        return None


@st.cache_data(ttl=3600)
def load_csv(subfolder, latest_date):
    """Load a processed CSV from S3."""
    path = f"{S3_BUCKET}/processed/{latest_date}/{subfolder}/{subfolder}.csv"
    try:
        with fs.open(path) as f:
            return pd.read_csv(f)
    except Exception:
        return pd.DataFrame()


def data_missing():
    st.error(
        "No processed data found in S3 under `processed/`. "
        "Run `process_reddit_data.py` first to generate and upload the CSV files."
    )
    st.stop()


# --- Detect latest date ---
latest_date = detect_latest_processed_date()
if not latest_date:
    data_missing()

# --- Load all datasets ---
topic_df      = load_csv("topic_analysis", latest_date)
sentiment_df  = load_csv("sentiment_by_topic", latest_date)
industry_df   = load_csv("posts_by_industry", latest_date)
salary_df     = load_csv("salary_stats", latest_date)
experience_df = load_csv("experience_distribution", latest_date)
skills_df     = load_csv("skills_summary", latest_date)
temporal_df   = load_csv("temporal_trends", latest_date)
metrics_df    = load_csv("network_metrics", latest_date)

if metrics_df.empty:
    data_missing()


# --- Helpers ---
def metric_val(name):
    row = metrics_df[metrics_df["metric"] == name]
    return int(row["value"].values[0]) if not row.empty else 0


# --- Header ---
st.title("Reddit CS Career Intelligence")
st.markdown("*Analyzing trends from r/csMajors and r/cscareerquestions using PySpark & AWS S3.*")

# --- Sidebar ---
st.sidebar.header("Pipeline Status")
st.sidebar.success(f"Data from: `{latest_date}`")
st.sidebar.divider()

all_topics = sorted(topic_df["topic"].unique().tolist()) if not topic_df.empty else []
selected_topics = st.sidebar.multiselect("Filter by Topic", all_topics, default=all_topics)

all_industries = sorted(industry_df["industry"].unique().tolist()) if not industry_df.empty else []
selected_industries = st.sidebar.multiselect("Filter by Industry", all_industries, default=all_industries)

# --- KPIs ---
total_posts    = metric_val("Total Posts")
total_comments = metric_val("Total Comments")

salary_posts = int(salary_df["salary_mention_posts"].sum()) if not salary_df.empty else 0

sentiment_counts = {}
if not sentiment_df.empty:
    for s in ["Positive", "Negative", "Neutral"]:
        sentiment_counts[s] = int(sentiment_df[sentiment_df["sentiment"] == s]["count"].sum())
dominant_sentiment = max(sentiment_counts, key=sentiment_counts.get) if sentiment_counts else "N/A"

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Posts", f"{total_posts:,}")
col2.metric("Total Comments", f"{total_comments:,}")
col3.metric("Salary Mentions", f"{salary_posts:,}")
col4.metric("Dominant Sentiment", dominant_sentiment)

st.divider()

# --- Row 1: Temporal trend + Topic distribution ---
row1_left, row1_right = st.columns([2, 1])

with row1_left:
    st.subheader("Sentiment Trend Over Time")
    if not temporal_df.empty:
        fig = px.line(
            temporal_df.sort_values("year_month"),
            x="year_month", y="post_count", color="sentiment",
            labels={"year_month": "Month", "post_count": "Posts", "sentiment": "Sentiment"},
            template="seaborn"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No temporal data available.")

with row1_right:
    st.subheader("Topic Distribution")
    filtered_topics = topic_df[topic_df["topic"].isin(selected_topics)] if not topic_df.empty else pd.DataFrame()
    if not filtered_topics.empty:
        fig = px.pie(filtered_topics, values="post_count", names="topic", hole=0.4)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No topic data available.")

st.divider()

# --- Row 2: Industry bar + Skills ---
row2_left, row2_right = st.columns(2)

with row2_left:
    st.subheader("Posts by Industry")
    filtered_ind = industry_df[industry_df["industry"].isin(selected_industries)] if not industry_df.empty else pd.DataFrame()
    if not filtered_ind.empty:
        fig = px.bar(
            filtered_ind.sort_values("post_count", ascending=True),
            x="post_count", y="industry", orientation="h",
            labels={"post_count": "Posts", "industry": "Industry"},
            template="seaborn"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No industry data available.")

with row2_right:
    st.subheader("Top Skills Mentioned")
    if not skills_df.empty:
        top_skills = skills_df.sort_values("skill_count", ascending=False).head(15)
        fig = px.bar(
            top_skills.sort_values("skill_count", ascending=True),
            x="skill_count", y="skill", orientation="h",
            labels={"skill_count": "Mentions", "skill": "Skill"},
            template="seaborn"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No skills data available.")

st.divider()

# --- Row 3: Sentiment by topic + Experience distribution ---
row3_left, row3_right = st.columns(2)

with row3_left:
    st.subheader("Sentiment by Topic")
    filtered_sent = sentiment_df[sentiment_df["topic"].isin(selected_topics)] if not sentiment_df.empty else pd.DataFrame()
    if not filtered_sent.empty:
        fig = px.bar(
            filtered_sent, x="topic", y="count", color="sentiment",
            barmode="group",
            labels={"count": "Posts", "topic": "Topic", "sentiment": "Sentiment"},
            template="seaborn"
        )
        fig.update_xaxes(tickangle=30)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No sentiment data available.")

with row3_right:
    st.subheader("Experience Level Distribution")
    if not experience_df.empty:
        fig = px.pie(experience_df, values="post_count", names="experience_level", hole=0.3)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No experience data available.")

st.divider()

# --- Salary section ---
st.subheader("Salary Mentions by Industry")
filtered_sal = salary_df[salary_df["industry"].isin(selected_industries)] if not salary_df.empty else pd.DataFrame()
if not filtered_sal.empty:
    display_sal = filtered_sal[["industry", "salary_mention_posts", "avg_engagement_score", "avg_comments"]].copy()
    display_sal.columns = ["Industry", "Posts w/ Salary", "Avg Score", "Avg Comments"]
    st.dataframe(display_sal.sort_values("Posts w/ Salary", ascending=False), use_container_width=True)
else:
    st.info("No salary data available.")

st.divider()
st.caption("Developed by Charlyne Dong, Cami Krugel, & Melanie Fernandez")
