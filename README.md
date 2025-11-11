# AWS S3 Sync Automation

A Python automation script that syncs files from a **local folder to AWS S3**, using checksum comparison to upload only changed files.

# Features
- Incremental uploads (modified files only)
- Checksum verification (data integrity)
- Logs (console + file)
- Multithreaded upload (faster sync)

---

# How to Run

# 1. Install dependency
```sh
pip install boto3
```

# 2. Configure AWS CLI
```sh
aws configure
```

Enter:
- AWS Access Key
- AWS Secret Key
- Region (example: `us-east-1`)

# 3. Run the script
```sh
python src/s3_sync.py --local-path ./my-folder --bucket my-bucket --prefix backups/project --profile default
```

---

# Project Structure
```
aws-s3-sync-automation/
│
├── src/
│   └── s3_sync.py
├── README.md
├── LICENSE
└── .gitignore
```

---

# Requirements
- Python 3.10+
- AWS CLI configured using `aws configure`
- IAM user with S3 upload permissions

---

Author: Elif Cetin
