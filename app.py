import streamlit as st
import pandas as pd
import plotly.express as px
import s3fs

st.set_page_config(page_title="Reddit CS Career Intelligence", layout="wide")

S3_BUCKET = "bigdata-cs-careers"

fs = s3fs.S3FileSystem(anon=True)


@st.cache_data(ttl=3600)
def get_all_processed_dates():
    """Return sorted list of all YYYY-MM-DD folders under processed/ in S3."""
    try:
        folders = fs.ls(f"{S3_BUCKET}/processed/")
        return sorted([f.split("/")[-1] for f in folders if f.split("/")[-1]])
    except Exception:
        return []


@st.cache_data(ttl=3600)
def load_csv(subfolder, date):
    """Load a single processed CSV from S3 for a specific date."""
    path = f"{S3_BUCKET}/processed/{date}/{subfolder}/{subfolder}.csv"
    try:
        with fs.open(path) as f:
            return pd.read_csv(f)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_all_dates(subfolder):
    """Concatenate a CSV from every available processed date."""
    dates = get_all_processed_dates()
    frames = [load_csv(subfolder, d) for d in dates]
    frames = [f for f in frames if not f.empty]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def data_missing():
    st.error(
        "No processed data found in S3 under `processed/`. "
        "Run `process_reddit_data.py` first to generate and upload the CSV files."
    )
    st.stop()


# --- Detect available dates ---
all_dates = get_all_processed_dates()
if not all_dates:
    data_missing()

# --- Load and aggregate all datasets across all dates ---
def _agg(subfolder, groupby, agg_dict):
    raw = load_all_dates(subfolder)
    if raw.empty:
        return pd.DataFrame()
    return raw.groupby(groupby, as_index=False).agg(**agg_dict)


topic_df = _agg("topic_analysis", "topic", {
    "post_count": ("post_count", "sum"),
    "avg_score": ("avg_score", "mean"),
    "avg_comments": ("avg_comments", "mean"),
})

sentiment_df = _agg("sentiment_by_topic", ["topic", "sentiment"], {
    "count": ("count", "sum"),
})

industry_df = _agg("posts_by_industry", "industry", {
    "post_count": ("post_count", "sum"),
    "avg_engagement_score": ("avg_engagement_score", "mean"),
    "avg_comments_count": ("avg_comments_count", "mean"),
})

experience_df = _agg("experience_distribution", "experience_level", {
    "post_count": ("post_count", "sum"),
})

skills_df = _agg("skills_summary", "skill", {
    "skill_count": ("skill_count", "sum"),
})

temporal_df = _agg("temporal_trends", ["year_month", "sentiment"], {
    "post_count": ("post_count", "sum"),
    "avg_score": ("avg_score", "mean"),
    "avg_comments": ("avg_comments", "mean"),
})

company_df = _agg("company_mentions", "company", {
    "mention_count": ("mention_count", "sum"),
    "avg_score": ("avg_score", "mean"),
    "avg_engagement": ("avg_engagement", "mean"),
})

topic_ind_df = _agg("topic_by_industry", ["industry", "topic"], {
    "post_count": ("post_count", "sum"),
    "avg_score": ("avg_score", "mean"),
})

skills_ind_df = _agg("skills_by_industry", ["industry", "skill"], {
    "skill_count": ("skill_count", "sum"),
})

# salary_stats needs special handling for optional median/min/max columns
_sal_raw = load_all_dates("salary_stats")
if not _sal_raw.empty:
    _sal_agg = {"salary_mention_posts": ("salary_mention_posts", "sum"),
                "avg_engagement_score": ("avg_engagement_score", "mean"),
                "avg_comments": ("avg_comments", "mean")}
    if "median_salary" in _sal_raw.columns:
        _sal_agg["median_salary"] = ("median_salary", "mean")
        _sal_agg["min_salary"] = ("min_salary", "min")
        _sal_agg["max_salary"] = ("max_salary", "max")
    salary_df = _sal_raw.groupby("industry", as_index=False).agg(**_sal_agg)
else:
    salary_df = pd.DataFrame()

# network_metrics: sum totals, average the averages
_met_raw = load_all_dates("network_metrics")
if not _met_raw.empty:
    _sum_rows = _met_raw[_met_raw["metric"].isin(["Total Posts", "Total Comments"])].groupby("metric", as_index=False)["value"].sum()
    _avg_rows = _met_raw[_met_raw["metric"].isin(["Avg Score", "Avg Comments"])].groupby("metric", as_index=False)["value"].mean()
    metrics_df = pd.concat([_sum_rows, _avg_rows], ignore_index=True)
else:
    metrics_df = pd.DataFrame()

if metrics_df.empty:
    data_missing()


# --- Helpers ---
def metric_val(name):
    row = metrics_df[metrics_df["metric"] == name]
    return int(row["value"].values[0]) if not row.empty else 0

# --- Fetch Theme Variables ---
# Pulling custom colors dynamically from .streamlit/config.toml
custom_colors = st.get_option("theme.chartCategoricalColors")
primary_color = st.get_option("theme.primaryColor")

# --- Header ---
st.title("Reddit CS Career Intelligence")
st.markdown("*Analyzing trends from r/csMajors and r/cscareerquestions using PySpark & AWS S3.*")

# --- Sidebar ---
st.sidebar.header("Pipeline Status")



# 1. Evaluate the health of all 8 expected datasets
data_health = {
    "Topic Analysis": not topic_df.empty,
    "Sentiment by Topic": not sentiment_df.empty,
    "Posts by Industry": not industry_df.empty,
    "Salary Stats": not salary_df.empty,
    "Experience Dist.": not experience_df.empty,
    "Skills Summary": not skills_df.empty,
    "Temporal Trends": not temporal_df.empty,
    "Network Metrics": not metrics_df.empty,
}

datasets_loaded = sum(data_health.values())
total_datasets = len(data_health)

# 2. Display high-level status indicator
if datasets_loaded == total_datasets:
    st.sidebar.success(f"**Healthy**: {datasets_loaded}/{total_datasets} datasets loaded")
elif datasets_loaded > 0:
    st.sidebar.warning(f"**Degraded**: Only {datasets_loaded}/{total_datasets} datasets loaded")
else:
    st.sidebar.error(f"**Critical**: {datasets_loaded}/{total_datasets} datasets loaded")

# 3. Add visual progress and partition date
if len(all_dates) == 1:
    st.sidebar.success(f"Data from: `{all_dates[0]}`")
else:
    st.sidebar.success(f"`{all_dates[0]}` → `{all_dates[-1]}`\n\n{len(all_dates)} days aggregated")
st.sidebar.progress(datasets_loaded / total_datasets)

# 4. Provide detailed diagnostic dropdown
with st.sidebar.expander("Diagnostic Details"):
    for name, is_loaded in data_health.items():
        icon = "🟢" if is_loaded else "🔴"
        st.write(f"{icon} {name}")

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
            color_discrete_sequence=custom_colors
        )
        st.plotly_chart(fig, width='stretch')
    else:
        st.info("No temporal data available.")

with row1_right:
    st.subheader("Topic Distribution")
    filtered_topics = topic_df[topic_df["topic"].isin(selected_topics)] if not topic_df.empty else pd.DataFrame()
    if not filtered_topics.empty:
        fig = px.pie(
            filtered_topics, 
            values="post_count", 
            names="topic", 
            hole=0.4,
            color_discrete_sequence=custom_colors
        )
        st.plotly_chart(fig, width='stretch')
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
            color_discrete_sequence=[primary_color]
        )
        st.plotly_chart(fig, width='stretch', key="posts_by_industry")
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
            color_discrete_sequence=[primary_color]
        )
        st.plotly_chart(fig, width='stretch', key="top_skills")
    else:
        st.info("No skills data available.")

st.divider()

# --- Skills by Industry ---
st.subheader("Skills by Industry")
st.markdown("*Select an industry to see which technical skills are discussed most.*")
if not skills_ind_df.empty:
    skill_industries = sorted(skills_ind_df["industry"].unique().tolist())
    selected_skill_industry = st.selectbox("Select Industry", skill_industries)
    filtered_si = skills_ind_df[skills_ind_df["industry"] == selected_skill_industry]
    top_si = filtered_si.sort_values("skill_count", ascending=False).head(15)
    fig = px.bar(
        top_si.sort_values("skill_count", ascending=True),
        x="skill_count", y="skill", orientation="h",
        labels={"skill_count": "Mentions", "skill": "Skill"},
        template="seaborn"
    )
    st.plotly_chart(fig, use_container_width=True, key="skills_by_industry")
else:
    st.info("No skills by industry data available. Re-run the processor to generate this dataset.")

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
            color_discrete_sequence=custom_colors
        )
        fig.update_xaxes(tickangle=30)
        st.plotly_chart(fig, width='stretch', key="sentiment_by_topic")
    else:
        st.info("No sentiment data available.")

with row3_right:
    st.subheader("Experience Level Distribution")
    if not experience_df.empty:
        fig = px.pie(
            experience_df, 
            values="post_count", 
            names="experience_level", 
            hole=0.3,
            color_discrete_sequence=custom_colors
        )
        st.plotly_chart(fig, width='stretch')
    else:
        st.info("No experience data available.")

st.divider()

# --- Row 4: Company Mentions + Topic by Industry ---
row4_left, row4_right = st.columns(2)

with row4_left:
    st.subheader("Top Company Mentions")
    if not company_df.empty:
        top_companies = company_df.sort_values("mention_count", ascending=False).head(15)
        fig = px.bar(
            top_companies.sort_values("mention_count", ascending=True),
            x="mention_count", y="company", orientation="h",
            labels={"mention_count": "Mentions", "company": "Company"},
            template="seaborn"
        )
        st.plotly_chart(fig, use_container_width=True, key="company_mentions")
    else:
        st.info("No company mention data available. Re-run the processor to generate this dataset.")

with row4_right:
    st.subheader("Topics by Industry")
    if not topic_ind_df.empty:
        filtered_ti = topic_ind_df[topic_ind_df["industry"].isin(selected_industries)]
        if not filtered_ti.empty:
            fig = px.bar(
                filtered_ti, x="industry", y="post_count", color="topic",
                barmode="group",
                labels={"post_count": "Posts", "industry": "Industry", "topic": "Topic"},
                template="seaborn"
            )
            fig.update_xaxes(tickangle=30)
            st.plotly_chart(fig, use_container_width=True, key="topics_by_industry")
        else:
            st.info("No data for selected industries.")
    else:
        st.info("No topic by industry data available. Re-run the processor to generate this dataset.")

st.divider()

# --- Salary section ---
st.subheader("Salary Mentions by Industry")
filtered_sal = salary_df[salary_df["industry"].isin(selected_industries)] if not salary_df.empty else pd.DataFrame()
if not filtered_sal.empty:
    if "median_salary" in filtered_sal.columns and filtered_sal["median_salary"].notna().any():
        sal_chart = filtered_sal.dropna(subset=["median_salary"]).sort_values("median_salary", ascending=True)
        fig = px.bar(
            sal_chart,
            x="median_salary", y="industry", orientation="h",
            labels={"median_salary": "Median Salary ($)", "industry": "Industry"},
            template="seaborn"
        )
        st.plotly_chart(fig, use_container_width=True, key="salary_by_industry")

    base_cols = ["industry", "salary_mention_posts", "avg_engagement_score", "avg_comments"]
    display_sal = filtered_sal[base_cols].copy()
    col_names = ["Industry", "Posts w/ Salary", "Avg Score", "Avg Comments"]
    if "median_salary" in filtered_sal.columns:
        display_sal["median_salary"] = filtered_sal["median_salary"]
        col_names.append("Median Salary ($)")
    display_sal.columns = col_names
    st.dataframe(display_sal.sort_values("Posts w/ Salary", ascending=False), use_container_width=True)
else:
    st.info("No salary data available.")

st.divider()
st.caption("Developed by Charlyne Dong, Cami Krugel, & Melanie Fernandez")
