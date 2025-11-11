# AWS S3 Sync Automation

Local folder → AWS S3 incremental sync automation  
Python script that uploads files from a local folder to AWS S3 using **Boto3**, with:

- Incremental upload (only changed files are uploaded)
- Checksum verification (ensures data integrity)
- Detailed logging (local log file + print to console)
- Multithreaded upload (faster sync)

---

# How to Run

1. Install dependencies
```sh

pip install boto3

# Configure aws configure
aws configure

# Enter:

AWS Access Key
AWS Secret Key
Region

# Run the script

python src/s3_sync.py --local-path ./my-folder --bucket my-bucket --prefix backups/project --profile default

# Project Structure

aws-s3-sync-automation/
│
├── src/
│   └── s3_sync.py     # Main sync script
├── README.md
├── LICENSE
└── .gitignore

# Requirements

Python 3.10+
AWS CLI configured (aws configure)
IAM user with S3 upload permissions

Author: Elif Cetin
