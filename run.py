#!/usr/local/bin/python

import logging
import os
import schedule
import subprocess
import tempfile
import time

from google.cloud import storage

# Set up logging
logging.basicConfig(format = '%(asctime)s %(message)s', level = logging.INFO)

# Upload file to google cloud storage

# Authentication is done via a service account keyfiles. Default location is
# /etc/creds.json. Customise via GOOGLE_APPLICATION_CREDENTIALS environment variable
# Set the bucket to store backups in via BUCKET

BUCKET = os.environ.get('BUCKET')
KEEP   = int(os.environ.get('KEEP', 5))

class GCS:
    def __init__(self, bucket, keep):
        self.keep = keep
        self.storage = storage.Client()

        if not self.storage.lookup_bucket(bucket):
            logging.critical("ERROR: bucket does not exist [%s]" % bucket)
            exit(1)
        else:
            self.bucket = self.storage.get_bucket(bucket)

    def upload(self, f, upload_name):
        blob = storage.Blob(upload_name, self.bucket)
        logging.info("Uploading backup to %s/%s" % (self.bucket, upload_name))
        blob.upload_from_file(f, rewind=True)

    def cleanup(self, prefix = ""):
        backups = list(self.bucket.list_blobs(prefix = prefix))
        to_delete = backups[:max(0, len(backups) - self.keep)]
        for backup in to_delete:
            backup.delete()

# How often to run backup
EVERY_N_DAYS  = int(os.environ.get('EVERY_N_DAYS', 1)) # default to once a day
AT_TIME       = os.environ.get('AT_TIME', "00:00") # at midnight

# DB configuration options
HOST      = os.environ.get('MYSQL_HOST', 'localhost')
PORT      = int(os.environ.get('MYSQL_PORT', 3306))
USERNAME  = os.environ.get('MYSQL_USER')
PASSWORD  = os.environ.get('MYSQL_PASSWORD')
ALL_DBS   = bool(os.environ.get('ALL_DATABASES', False))
DB        = "" if ALL_DBS else os.environ.get('MYSQL_DATABASE')

cloud = GCS(BUCKET, KEEP)

def db_backup():
    backup_name = "%s/%s-%s.sql" % (DB, DB, time.strftime("%Y-%m-%d-%H%M%S"))
    logging.info("Running backup %s" % backup_name)
    with tempfile.NamedTemporaryFile() as f:
        subprocess.run([
            "mysqldump",
            "--host=%s" % HOST,
            "--port=%s" % PORT,
            "--user=%s" % USERNAME,
            "--password=%s" % PASSWORD,
            "--single-transaction",
            "--hex-blob",
            DB], stdout = f)

        cloud.upload(f, backup_name)

    cloud.cleanup(DB)

def run_backup():
    global DB
    if ALL_DBS:
        database_list_command = "mysql -u %s -p%s -h %s -P %s --silent -N -e 'show databases'" % (USERNAME, PASSWORD, HOST, PORT)
        for database in os.popen(database_list_command).readlines():
            database = database.strip()
            if database == 'information_schema':
                continue
            if database == 'performance_schema':
                continue
            DB = database
            db_backup()
    else:
        db_backup()

if EVERY_N_DAYS > 0:
    schedule.every(EVERY_N_DAYS).days.at(AT_TIME).do(run_backup)
    while True:
        schedule.run_pending()
        time.sleep(30)
else:
    run_backup()
