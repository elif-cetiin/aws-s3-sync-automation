#!/usr/bin/env python3
"""
s3_sync.py
Simple, safe sync script from local folder -> S3 with checksum-based uploads.

Usage:
  python src/s3_sync.py --local-path ./my-folder --bucket my-bucket --prefix backups/2025-09-01 --profile default --dry-run
"""
import os
import sys
import argparse
import logging
import hashlib
import mimetypes
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import boto3
from botocore.config import Config
from boto3.s3.transfer import TransferConfig

# constants
DEFAULT_CONCURRENCY = 4
CHUNK_SIZE = 8 * 1024 * 1024  # 8MB

logger = logging.getLogger("s3_sync")
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

def md5_for_file(path, chunk_size=8192):
    h = hashlib.md5()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()

def s3_object_md5(head_obj):
    meta = head_obj.get('Metadata', {})
    if 'md5' in meta:
        return meta['md5']
    etag = head_obj.get('ETag', '').strip('"')
    if '-' not in etag:
        return etag
    return None

def iter_local_files(local_root, ignore_hidden=True):
    root = Path(local_root)
    for p in root.rglob("*"):
        if p.is_file():
            if ignore_hidden and any(part.startswith('.') for part in p.parts):
                continue
            yield p

class S3Sync:
    def __init__(self, bucket, prefix="", profile=None, region=None, concurrency=DEFAULT_CONCURRENCY, dry_run=False):
        session_args = {}
        if profile:
            session_args['profile_name'] = profile
        session = boto3.Session(**session_args) if session_args else boto3.Session()
        s3_config = Config(retries={"max_attempts": 10, "mode": "standard"})
        self.s3 = session.resource('s3', config=s3_config, region_name=region)
        self.client = session.client('s3', config=s3_config, region_name=region)
        self.bucket = self.s3.Bucket(bucket)
        self.prefix = prefix.strip("/")
        self.dry_run = dry_run
        self.concurrency = max(1, concurrency)
        self.transfer_config = TransferConfig(
            multipart_threshold=CHUNK_SIZE,
            multipart_chunksize=CHUNK_SIZE,
            max_concurrency=self.concurrency,
            use_threads=True
        )

    def s3_key_for(self, local_root, path: Path):
        rel = str(path.relative_to(local_root)).replace("\\", "/")
        if self.prefix:
            return f"{self.prefix}/{rel}"
        return rel

    def fetch_s3_index(self):
        logger.info("Indexing S3 objects under prefix '%s'...", self.prefix or "<root>")
        objs = {}
        kwargs = {'Bucket': self.bucket.name, 'Prefix': self.prefix + '/' if self.prefix else ''}
        paginator = self.client.get_paginator('list_objects_v2')
        for page in paginator.paginate(**kwargs):
            for item in page.get('Contents', []):
                key = item['Key']
                try:
                    head = self.client.head_object(Bucket=self.bucket.name, Key=key)
                    objs[key] = head
                except Exception as e:
                    logger.warning("Could not head object %s: %s", key, e)
        logger.info("Indexed %d objects from S3", len(objs))
        return objs

    def upload_file(self, local_root, path: Path, s3_index):
        key = self.s3_key_for(local_root, path)
        s3_head = s3_index.get(key)
        local_md5 = md5_for_file(path)
        needs_upload = True

        if s3_head:
            s_md5 = s3_object_md5(s3_head)
            if s_md5 and s_md5 == local_md5:
                needs_upload = False

        if not needs_upload:
            logger.debug("Skip (unchanged): %s -> %s", path, key)
            return ("skip", path, key)

        if self.dry_run:
            logger.info("[DRY-RUN] Would upload: %s -> %s", path, key)
            return ("dry-run", path, key)

        content_type, _ = mimetypes.guess_type(str(path))
        extra_args = {'Metadata': {'md5': local_md5}}
        if content_type:
            extra_args['ContentType'] = content_type

        try:
            logger.info("Uploading: %s -> s3://%s/%s", path, self.bucket.name, key)
            self.bucket.upload_file(
                Filename=str(path),
                Key=key,
                ExtraArgs=extra_args,
                Config=self.transfer_config
            )
            return ("uploaded", path, key)
        except Exception as e:
            logger.exception("Failed to upload %s: %s", path, e)
            return ("error", path, key, str(e))

    def delete_extra(self, s3_index, local_keys_set):
        to_delete = [k for k in s3_index.keys() if k not in local_keys_set]
        if not to_delete:
            logger.info("No remote keys to delete.")
            return []
        deleted = []
        for key in to_delete:
            if self.dry_run:
                logger.info("[DRY-RUN] Would delete s3://%s/%s", self.bucket.name, key)
                deleted.append(key)
                continue
            try:
                logger.info("Deleting s3://%s/%s", self.bucket.name, key)
                self.client.delete_object(Bucket=self.bucket.name, Key=key)
                deleted.append(key)
            except Exception as e:
                logger.exception("Failed to delete %s: %s", key, e)
        return deleted

    def sync(self, local_root, delete=False):
        local_root = Path(local_root).resolve()
        if not local_root.exists():
            raise RuntimeError("Local path does not exist: %s" % local_root)
        local_files = list(iter_local_files(local_root))
        logger.info("Found %d local files under %s", len(local_files), local_root)

        s3_index = self.fetch_s3_index()
        local_to_key = {self.s3_key_for(local_root, p): p for p in local_files}
        local_keys_set = set(local_to_key.keys())

        results = []
        with ThreadPoolExecutor(max_workers=self.concurrency) as ex:
            futures = [ex.submit(self.upload_file, local_root, p, s3_index) for p in local_files]
            for fut in as_completed(futures):
                results.append(fut.result())

        uploaded = [r for r in results if r[0] == 'uploaded']
        skipped = [r for r in results if r[0] == 'skip']
        errors = [r for r in results if r[0] == 'error']
        dry = [r for r in results if r[0] == 'dry-run']

        logger.info("Summary: uploaded=%d, skipped=%d, errors=%d, dry=%d", len(uploaded), len(skipped), len(errors), len(dry))

        deleted = []
        if delete:
            deleted = self.delete_extra(s3_index, local_keys_set)

        return {
            "uploaded": uploaded,
            "skipped": skipped,
            "errors": errors,
            "deleted": deleted,
            "dry": dry
        }

def main():
    parser = argparse.ArgumentParser(description="Sync local folder to S3 with checksum detection")
    parser.add_argument("--local-path", "-l", required=True, help="Local directory to upload")
    parser.add_argument("--bucket", "-b", required=True, help="S3 bucket name")
    parser.add_argument("--prefix", "-p", default="", help="S3 prefix (folder) under the bucket")
    parser.add_argument("--profile", help="AWS profile (from ~/.aws/credentials)")
    parser.add_argument("--region", help="AWS region")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    parser.add_argument("--dry-run", action="store_true", help="Don't upload, just report")
    parser.add_argument("--delete", action="store_true", help="Delete remote keys not present locally")
    parser.add_argument("--verbose", action="store_true", help="Verbose logging")
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    syncer = S3Sync(bucket=args.bucket, prefix=args.prefix, profile=args.profile,
                    region=args.region, concurrency=args.concurrency, dry_run=args.dry_run)

    result = syncer.sync(args.local_path, delete=args.delete)
    import json, datetime
    stamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    logfile = Path(args.local_path) / f".s3_sync_{stamp}.json"
    with open(logfile, "w", encoding="utf-8") as wf:
        json.dump({
            "uploaded": len(result["uploaded"]),
            "skipped": len(result["skipped"]),
            "errors": len(result["errors"]),
            "deleted": len(result["deleted"]),
            "details": {
                "uploaded": [[str(x[1]), x[2]] for x in result["uploaded"]],
                "skipped": [[str(x[1]), x[2]] for x in result["skipped"]],
                "errors": result["errors"],
                "deleted": result["deleted"]
            }
        }, wf, indent=2)
    logger.info("Wrote run log to %s", logfile)

if __name__ == "__main__":
    main()
