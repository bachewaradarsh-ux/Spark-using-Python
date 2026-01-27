import logging
import boto3
from datetime import datetime

class S3Logger:
    def __init__(self, job_name, bucket_name="project-data-adarshpractice"):
        """
        Initialize the logger.
        :param job_name: Name of the script/job (used as subfolder in logs).
        :param bucket_name: Same bucket as input files.
        """
        self.bucket_name = bucket_name
        self.job_name = job_name
        self.log_entries = []

        # Configure console logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        self.logger = logging.getLogger(job_name)

    def log(self, level, message):
        """
        Log a message with given level.
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        entry = f"{timestamp} [{level}] {self.job_name}: {message}"
        self.log_entries.append(entry)

        if level == "INFO":
            self.logger.info(message)
        elif level == "WARN":
            self.logger.warning(message)
        elif level == "ERROR":
            self.logger.error(message)
        elif level == "DEBUG":
            self.logger.debug(message)

    def capture_spark_metadata(self, spark):
        """
        Capture Spark application metadata for audit.
        """
        app_id = spark.sparkContext.applicationId
        master = spark.sparkContext.master
        conf = spark.sparkContext.getConf().getAll()

        self.log("INFO", f"Spark App ID: {app_id}")
        self.log("INFO", f"Spark Master: {master}")
        self.log("INFO", f"Spark Config: {conf}")

    def flush_to_s3(self):
        """
        Write all collected logs to S3 inside logs/job_name folder.
        """
        s3 = boto3.client("s3")
        ts = datetime.now().strftime("%Y%m%d%H%M%S")
        key = f"logs/{self.job_name}/{self.job_name}_{ts}.log"

        log_data = "\n".join(self.log_entries)

        s3.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=log_data.encode("utf-8")
        )
        print(f"Logs written to s3://{self.bucket_name}/{key}")