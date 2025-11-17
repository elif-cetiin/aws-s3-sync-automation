# AWS S3 Sync Automation

A production-grade command-line utility that synchronizes a local directory with an Amazon S3 bucket using checksum-based change detection, enabling efficient, secure, and reliable backup operations.

---

## Features

- Incremental uploads, uploads only modified files using MD5 checksum comparison  
- Checksum verification, ensures data integrity between local and S3 objects  
- Concurrent multipart uploads, faster transfer using multithreaded upload  
- Dry-run mode, preview actions without modifying S3  
- Automatic cleanup, optional removal of remote files not present locally  
- Structured logging, console and JSON summary output of every run  

---

## Requirements

- Python 3.10+
- AWS CLI configured using `aws configure`
- IAM user with S3 read/write permissions (ListBucket, PutObject, DeleteObject)

---


## Project Structure

```
aws-s3-sync-automation/
│
├── src/
│   └── s3_sync.py
├── logs/
│   └── log_2025-11-16.json
├── .gitignore
├── LICENSE
└── README.md
```

---

## Installation

### Install AWS CLI
https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html

### Verify installation
```sh
aws --version
pip install boto3

```
## Usage
```sh
python src/s3_sync.py --local-path ./my-folder --bucket my-bucket --prefix backups/project --dry-run --verbose
python src/s3_sync.py --local-path ./my-folder --bucket my-bucket --prefix backups/project
python src/s3_sync.py --local-path ./my-folder --bucket my-bucket --delete
```

Author
Elif Cetin
