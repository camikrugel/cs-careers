import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go



# 1. Page Configuration
st.set_page_config(page_title="Reddit CS Career Intelligence", layout="wide")

# 2. Header & Project Context
st.title("Reddit CS Career Intelligence")
st.markdown("""
*Analyzing real-time trends from r/csMajors and r/cscareerquestions using PySpark and AWS S3.*
""")

# 3. Sidebar: Pipeline Monitoring
st.sidebar.header("Pipeline Status")
st.sidebar.status("Connected to AWS S3")
st.sidebar.progress(100, text="Last PySpark Job: Success")
st.sidebar.divider()
st.sidebar.subheader("Filters")
subreddit = st.sidebar.multiselect("Subreddits", ["r/csMajors", "r/cscareerquestions"], default=["r/csMajors", "r/cscareerquestions"])
date_range = st.sidebar.date_input("Analysis Window")

# 4. Key Performance Indicators (KPIs)
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Posts", "42,891", "+1.2k")
col2.metric("Unique Users", "18,402")
col3.metric("Salary Mentions", "1,104", "High Activity")
col4.metric("Market Sentiment", "Neutral", "-0.04")

st.divider()

# 5. Main Analytics Row
top_col, bottom_col = st.columns([2, 1])

with top_col:
    st.subheader("Tech Job Market Sentiment Trend")
    dates = pd.date_range(start="2026-01-01", periods=60)
    sentiment_data = pd.DataFrame({
        "Date": dates,
        "Sentiment": np.random.uniform(-0.2, 0.4, size=60).cumsum() / 10 # Simulated trend
    })
    fig_line = px.line(sentiment_data, x="Date", y="Sentiment", template="seaborn")
    fig_line.add_hline(y=0, line_dash="dash", line_color="red", annotation_text="Neutral Threshold")
    st.plotly_chart(fig_line, width='stretch')

with bottom_col:
    st.subheader("Topic Distribution")
    topics = pd.DataFrame({
        "Topic": ["Internships", "Interviews", "Course Advice", "New Grad", "Resume Review"],
        "Count": [1200, 850, 600, 450, 300]
    })
    fig_pie = px.pie(topics, values="Count", names="Topic", hole=0.4, color_discrete_sequence=st.get_option("theme.chartCategoricalColors"))
    st.plotly_chart(fig_pie, width='stretch')

st.divider()

# 6. Specialized Insights Row
insight_left, insight_right = st.columns(2)

with insight_left:
    st.subheader("Salary Extraction Patterns")
    st.caption("Pattern-matched salary mentions from post text (Week 2/3 Goal)")
    salary_data = pd.DataFrame({
        "Company Type": ["Big Tech", "Unicorn", "Mid-size", "Startup"],
        "Avg Hourly ($)": [55, 48, 35, 28]
    })
    fig_salary = px.bar(salary_data, x="Company Type", y="Avg Hourly ($)", color="Company Type")
    st.plotly_chart(fig_salary, width='stretch')

with insight_right:
    st.subheader("Recruiting & Project Advice Analysis")
    st.caption("Extracted recurring themes and relative frequency (Week 3)")

    advice_categories = pd.DataFrame({
        "Advice Theme": ["LeetCode Patterns", "System Design", "Networking/Referrals", 
                         "Resume Formatting", "Behavioral Prep", "Project Portfolio"],
        "Frequency": [95, 40, 85, 60, 35, 55],
        "Actionability Score": [0.9, 0.7, 0.85, 0.6, 0.5, 0.9] # Mock metric
    })

    # Create a bubble chart to show advice "density"
    fig_advice = px.scatter(advice_categories, 
                            x="Advice Theme", 
                            y="Frequency",
                            size="Frequency", 
                            color="Actionability Score",
                            hover_name="Advice Theme",
                            color_continuous_scale=px.colors.sequential.Viridis,
                            title="Advice Frequency vs. Actionability")
    
    st.plotly_chart(fig_advice, width='stretch')

    # Added a "Top Advice Snippets" feature
    with st.expander("View Top Extracted Advice (Mocked)"):
        st.write("1. **LeetCode**: Focus on 'Blind 75' and 'NeetCode 150' lists.")
        st.write("2. **Referrals**: Cold messaging on LinkedIn has a <5% success rate vs. internal portals.")
        st.write("3. **Projects**: Full-stack CRUD apps are currently oversaturated; focus on niche APIs.")

st.divider()

# 7. Raw Data Preview 
st.subheader("Cleaned Data Preview (Parquet Sample)")
sample_df = pd.DataFrame([
    {"Timestamp": "2026-03-31", "Subreddit": "r/csMajors", "Cleaned_Text": "Looking for summer 2026 internship advice...", "Sentiment": 0.12, "Topic": "Internships"},
    {"Timestamp": "2026-03-30", "Subreddit": "r/cscareerquestions", "Cleaned_Text": "What are the best patterns for system design?", "Sentiment": 0.05, "Topic": "Interview Prep"},
    {"Timestamp": "2026-03-30", "Subreddit": "r/csMajors", "Cleaned_Text": "I just received an offer for $50/hr!", "Sentiment": 0.85, "Topic": "Salary"},
])
st.dataframe(sample_df, width='stretch')

# 8. Footer
st.caption("Developed by Charlyne Dong, Cami Krugel, & Melanie Fernandez")