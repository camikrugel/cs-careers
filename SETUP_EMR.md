# Quick Start: EMR Setup & Execution

## TLDR (5 minutes to submit job)

### 1. Create EMR Cluster
Go to **AWS Console → EMR → Create Cluster**

Configure:
- **Name:** Reddit Career Analysis
- **EMR Release:** 6.15.0 (or later)
- **Applications:** Spark
- **Instance Type:** m5.xlarge
- **Instance Count:** 3
- **Cluster logs:** uncheck "Publish cluster-specific logs to Amazon S3"
- **Key Pair:** vockey (make a key pair + make sure to save it securely)
- **EMR Service Role:** EMR_DefaultRole
- **EMR Instance Profile:** EMR_EC2_DefaultRole

Click **Create** and wait for "Waiting" status (~10 min)

### 2. Add Spark Job Step
In AWS Console:
1. Open your cluster
2. Click **Add Step**
3. Configure:
   - **Type:** Spark application
   - **Location:** `s3://bigdata-cs-careers/scripts/process_reddit_data.py`
   - **Arguments:** `--mode emr --s3-bucket bigdata-cs-careers --local-output-dir`
   - **Step action:** Continue
4. Click **Add Step**

### 3. Wait for Completion
Monitor in AWS Console → Steps section (~10-15 min)

### 4. Download Results
Once complete:
```bash
# Get master node public IP from AWS Console
# Then download results:
scp -r ec2-user@<master-public-ip>:/tmp/reddit-results/* data/processed/
```

### 5. Terminate Cluster
```bash
aws emr terminate-clusters --cluster-ids j-XXXXX
```

---

## Full Details

See **data-processing/README_EMR.md** for:
- Detailed AWS setup
- SSH access & SCP download instructions
- Job monitoring
- Troubleshooting
- IAM permissions (S3 READ-ONLY for input)

---

## Local Testing (No EMR)

```bash
# Run locally with local files
python data-processing/scripts/process_reddit_data.py --mode local

# Results: data/processed/
```

## Support

- **Issues?** Check data-processing/README_EMR.md Troubleshooting section
- **AWS Docs:** https://docs.aws.amazon.com/emr/
- **PySpark:** https://spark.apache.org/docs/latest/api/python/

