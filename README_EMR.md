# EMR Setup & Execution Guide - Detailed

**Key Point:** Results are saved locally on the EMR cluster and downloaded manually. Only S3 READ permission needed for input data from the public bucket.

---

## Prerequisites

### 1. AWS Learner Lab Access
- Access to AWS Console (Learner Lab)
- Input data is in public S3 bucket: `bigdata-cs-careers` (no special permissions needed)
- AWS CLI installed and configured (optional for advanced users)

### 2. Install AWS CLI (Optional)

If you want to use CLI commands for monitoring:

**macOS:**
```bash
brew install awscli
```

**Ubuntu/Debian:**
```bash
sudo apt-get install awscli
```

**Windows:**
```bash
pip install awscli
```

Verify installation:
```bash
aws --version
```

### 3. Configure AWS CLI (Optional)

In AWS Learner Lab:
1. Click "AWS Details" 
2. Copy AWS CLI credentials
3. Run `aws configure` and paste them

```bash
aws configure
# Paste: AWS Access Key ID, Secret Access Key, Region (us-east-1)
```

---

## Step 1: Create EMR Cluster

### Via AWS Console (Recommended)

1. **Go to EMR Service**
   - Search for "EMR" in AWS Console
   - Click "Create cluster"

2. **Configure Cluster**
   - **Cluster name:** Reddit Career Analysis
   - **EMR Release:** 6.15.0 (or newer)
   - **Applications:** Spark

3. **Instance Configuration**
   - **Instance Type:** m5.xlarge
   - **Instance Count:** 3 (1 master, 2 workers)
   - **Key Pair:** vockey (IMPORTANT: this enables SSH access)
   - **EMR Service Role:** EMR_DefaultRole

4. **Additional Options**
   - Auto-terminate: After job completion (recommended)

5. **Click Create**
   - Wait for "Waiting" status (~10 minutes)
   - Note the **Cluster ID**: `j-XXXXX` 

### Via AWS CLI

```bash
aws emr create-cluster \
  --name "Reddit Career Analysis" \
  --release-label emr-6.15.0 \
  --applications Name=Spark \
  --instance-count 3 \
  --instance-type m5.xlarge \
  --ec2-attributes KeyName=vockey
```

---

## Step 2: Submit Spark Job

### Via AWS Console (Recommended)

1. Open your cluster in EMR console
2. Click **Steps** tab
3. Click **Add step**
4. Configure:
   - **Step type:** Spark application
   - **Application location:** `s3://bigdata-cs-careers/scripts/process_reddit_data.py`
   - **Arguments:** (leave empty)
   - **Step action on failure:** Continue

5. Click **Add**
6. Job will appear in Steps list with status

### Via AWS CLI

```bash
aws emr add-steps \
  --cluster-id j-XXXXX \
  --steps Type=Spark,Name=reddit-analysis,SparkSubmitParameters="--packages org.apache.hadoop:hadoop-aws:3.3.0",ActionOnFailure=CONTINUE,Args=[s3://bigdata-cs-careers/scripts/process_reddit_data.py,--mode,emr,--s3-bucket,bigdata-cs-careers]
```

---

## Step 3: Monitor Progress

### Via AWS Console

1. Open your cluster
2. Click **Steps** tab
3. Watch for:
   - **Status:** RUNNING → COMPLETED (or FAILED)
   - **Progress:** Shows step execution progress
   - Typical time: 10-15 minutes

### Via AWS CLI

Check step status:
```bash
aws emr describe-step \
  --cluster-id j-XXXXX \
  --step-id s-XXXXX

# Output: "RUNNING", "COMPLETED", or "FAILED"
```

### SSH to Cluster (Advanced)

Connect to master node for real-time logs:
```bash
# Get master public IP from AWS Console > Cluster > Master node
ssh -i ~/.ssh/vockey.pem ec2-user@<master-public-ip>

# View Spark logs
tail -f /var/log/spark/apps/*.log
```

---

## Step 4: Download Results

Once job completes, results are in `/tmp/reddit-results/` on EMR master node.

### Using SCP (Recommended)

1. Get **Master Public IP** from AWS Console:
   - EMR → Cluster → Click your cluster → Master tab

2. Download files:
```bash
# From your local machine
scp -r -i ~/.ssh/vockey.pem \
  ec2-user@<master-public-ip>:/tmp/reddit-results/* \
  data/processed/
```

Replace `<master-public-ip>` with actual IP from console.

### Using AWS Systems Manager Session Manager (Alternative)

If SSH doesn't work:
```bash
aws ssm start-session --target <instance-id>
# Then inside the session:
cd /tmp/reddit-results
# View files, or copy to local via S3
```

### Verify Download

After SCP completes:
```bash
ls -la data/processed/
# Should show 8 folders with CSV files:
# topic_analysis/
# sentiment_by_topic/
# posts_by_industry/
# salary_stats/
# experience_distribution/
# skills_summary/
# temporal_trends/
# network_metrics/
```

---

## Step 5: Terminate Cluster

**Important:** Terminate cluster to avoid ongoing charges!

### Via AWS Console

1. EMR → Clusters
2. Select your cluster
3. Click **Terminate**
4. Confirm "Terminate cluster"

### Via AWS CLI

```bash
aws emr terminate-clusters --cluster-ids j-XXXXX
```

---

## Running Locally (Testing First)

Before running on EMR, test locally:

```bash
# Create environment
conda env create -f environment.yml
conda activate reddit-career-analysis

# Run local job
python data-processing/scripts/process_reddit_data.py --mode local

# Check output
ls -la data/processed/
```

---

## Troubleshooting

### Job Failed / Status: FAILED

**Step 1: Check Error Message**
- AWS Console → Steps → Click failed step → Check error

**Step 2: Common Issues**

| Error | Solution |
|-------|----------|
| "Hadoop home directory not found" | Normal on Windows. Use Pandas writer (already configured). |
| "s3://bigdata-cs-careers/raw_posts.json: No such file" | Verify S3 bucket exists and is named correctly. |
| "Permission denied: s3://..." | Check S3 bucket policy. Data bucket is public READ-only. |
| "PySpark not found" | EMR should have Spark. Verify release label is correct. |

**Step 3: View Detailed Logs**

SSH to cluster:
```bash
# Master logs
cat /mnt/var/log/spark/apps/*stdout

# Executor logs
ls /var/log/hadoop/userlogs/
```

### Can't SSH to Cluster

**Problem:** "Permission denied" when using SSH

**Solutions:**
1. Verify key pair: `ls ~/.ssh/vockey.pem`
2. Check permissions: `chmod 600 ~/.ssh/vockey.pem`
3. Verify security group allows SSH (port 22)
4. Use AWS Systems Manager instead

### Results Not Showing Up

**Step 1: Check Job Status**
```bash
aws emr describe-step --cluster-id j-XXXXX --step-id s-XXXXX
```

**Step 2: If Still Running**
- Wait (typically 10-15 minutes)
- Monitor via console

**Step 3: If Failed**
- Check Step 1 error message
- Verify input data exists in S3

**Step 4: Verify Cluster Still Running**
- EMR console should show "Waiting" status
- If "Terminated", restart cluster and re-submit job

### SCP Download Fails

**Problem:** "No such file or directory" when downloading

**Solutions:**
1. Verify master public IP is correct (not internal IP)
2. Check key pair: `-i ~/.ssh/vockey.pem`
3. Verify SSH works first: `ssh -i ~/.ssh/vockey.pem ec2-user@<ip>`
4. Check results folder: `ssh ec2-user@<ip> ls /tmp/reddit-results/`

---

## Support

### AWS Documentation
- EMR: https://docs.aws.amazon.com/emr/
- S3: https://docs.aws.amazon.com/s3/
- EC2 Key Pairs: https://docs.aws.amazon.com/ec2/latest/userguide/ec2-key-pairs.html

### Spark Documentation
- PySpark: https://spark.apache.org/docs/latest/api/python/
- Spark SQL: https://spark.apache.org/docs/latest/sql-programming-guide.html

### Project Documentation
- Quick Start: See SETUP_EMR.md
- Data Schema: See data-processing/docs/data_schema.md
- Architecture: See EMR_IMPLEMENTATION.md

---

## Checklist for Success

Before submitting job:
- [ ] AWS Learner Lab credentials configured
- [ ] Cluster created and showing "Waiting" status
- [ ] Master public IP is accessible
- [ ] SSH key pair (vockey) downloaded
- [ ] Security group allows SSH (port 22)

After job completes:
- [ ] Check step status is "COMPLETED"
- [ ] Download results via SCP
- [ ] Verify 8 CSV files in data/processed/
- [ ] Terminate cluster (avoid charges)
- [ ] Check CSV files have data (not empty)

---

## Quick Reference

| Task | Command |
|------|---------|
| Create cluster | AWS Console → EMR → Create cluster |
| Submit job | AWS Console → Steps → Add step |
| Check status | `aws emr describe-step --cluster-id j-XXXXX --step-id s-XXXXX` |
| Download results | `scp -r ec2-user@<ip>:/tmp/reddit-results/* data/processed/` |
| Terminate cluster | `aws emr terminate-clusters --cluster-ids j-XXXXX` |
| SSH to cluster | `ssh -i ~/.ssh/vockey.pem ec2-user@<master-ip>` |
| View S3 data | `aws s3 ls s3://bigdata-cs-careers/` |

---

## Version Info

- **EMR Release:** 6.15.0+
- **Spark Version:** 3.5.0
- **Python:** 3.9+
- **Last Updated:** 2024
