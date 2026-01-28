import logging
import boto3
import json
from datetime import datetime
from pyspark.sql import SparkSession

class ProjectLogger:
    def __init__(self, job_name, bucket_name="project-data-adarshpractice"):
        self.job_name = job_name
        self.bucket_name = bucket_name
        self.session_ts = datetime.now().strftime("%Y%m%d%H%M%S")
        self.s3 = boto3.client("s3")
        self.log_entries = []

        # Console logging (still goes to CloudWatch automatically in Glue/EMR)
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        self.logger = logging.getLogger(job_name)

    def _s3_key(self):
        return f"telekom_project/logs/{self.job_name}/{self.job_name}_{self.session_ts}.json"

    def log(self, level, message, extra=None):
        entry = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "level": level,
            "job_name": self.job_name,
            "message": message,
            "extra": extra or {}
        }
        self.log_entries.append(entry)

        # Console logging
        if level == "INFO":
            self.logger.info(message)
        elif level == "WARN":
            self.logger.warning(message)
        elif level == "ERROR":
            self.logger.error(message)
        elif level == "DEBUG":
            self.logger.debug(message)

    def capture_spark_metadata(self, spark):
        app_id = spark.sparkContext.applicationId
        master = spark.sparkContext.master
        conf = dict(spark.sparkContext.getConf().getAll())
        event_log_dir = spark.sparkContext.getConf().get("spark.eventLog.dir", None)

        self.log("INFO", "Spark Metadata", {
            "app_id": app_id,
            "master": master,
            "conf": conf,
            "event_log_dir": event_log_dir
        })

    def flush_to_s3(self):
        log_data = "\n".join(json.dumps(entry) for entry in self.log_entries)
        self.s3.put_object(
            Bucket=self.bucket_name,
            Key=self._s3_key(),
            Body=log_data.encode("utf-8")
        )
        print(f"Audit logs written to s3://{self.bucket_name}/{self._s3_key()}")