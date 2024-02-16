#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import json
import os
import glob
from datetime import datetime
from pathlib import Path
import sys
import ulid

from pyspark.sql.types import *
import pyspark.sql.functions as F

from delta.tables import DeltaTable
from delta.tables import *

from tlhop.datasets import DataSets
from tlhop.library import *


class CensysDatasetManager(object):
    
    _ERROR_MESSAGE_001 = "None active Spark session was found. Please start a new Spark session before use the API."
    
    def __init__(self, output_folder, output_log=None):
        """
        A Class to convert Censys's Avro files into a single Delta format, to enrich and manage the dataset.
        
        :params output_folder: A path where the converted dataset will be stored;
        :param output_log: A filepath to store the log execution (default None, only prints on screen).
        """
        
        self.spark_session = SparkSession.getActiveSession()
        if not self.spark_session:
            raise Exception(self._ERROR_MESSAGE_001)
            
        self.LOG_OUTPUT = output_log
        self.OUTPUT_FOLDER = output_folder
        self.locationModel = {}
        
        # Setting Spark configurations
        self.spark_session.conf.set("spark.sql.caseSensitive", "true")
        self.spark_session.conf.set("spark.sql.parquet.compression.codec", "gzip")
        self.n_cores = self.spark_session._sc.defaultParallelism
        self.spark_session.conf.set("spark.sql.files.minPartitionNum", self.n_cores * 3)
        
        # Setting Delta configurations
        minFileSize = 200 * 1024 * 1024
        maxFileSize = 256 * 1024 * 1024
        maxThreads = 10
        
        self.spark_session.conf.set("spark.databricks.delta.optimize.minFileSize", minFileSize)
        self.spark_session.conf.set("spark.databricks.delta.optimize.maxFileSize", maxFileSize)
        self.spark_session.conf.set("spark.databricks.delta.optimize.maxThreads", maxThreads)
        self.spark_session.conf.set("spark.databricks.delta.retentionDurationCheck.enabled","false")
        self.spark_session.conf.set("spark.databricks.delta.merge.repartitionBeforeWrite.enabled", "true") 
        
    def convert_files(self, snapshot_input_folder, filter_by_contry='Brazil'):
        """
        Converts all Avro files in `snapshot_input_folder` into a single DELTA file format.
        
        :params snapshot_input_folder: The snapshot folder;
        :params filter_by_contry: Filter only records of a specific country (default, Brazil). If None, it will save the complete data. 
        """
        
        filename = os.path.basename(snapshot_input_folder)
        self._logger("[INFO] Reading {} and saving as {}".format(filename, self.OUTPUT_FOLDER))

        t1 = time.time()
        self._convert(snapshot_input_folder, self.OUTPUT_FOLDER, filter_by_contry)
        t2 = time.time()
        
        elapsed_time = int(t2-t1)
        self._logger("[INFO] Total time is {} seconds.".format(elapsed_time))

    
    def optimize_delta(self, dataset_folder):
        """
        Spark may generate small files over time. This method optimize Delta files by merging small files.
        
        :params dataset_folder: Dataset's filepath 
        """
        
        deltaTable = DeltaTable.forPath(self.spark_session, dataset_folder)
        t1 = time.time()
        self._logger(f"[INFO] Starting to optimize delta table `{dataset_folder}`")
        deltaTable.optimize().executeCompaction()
        t2 = time.time()
        elapsed_time = int(t2-t1)
        self._logger(f"[INFO] Optimization finished in {elapsed_time} seconds.")
    
    def remove_old_delta_versions(self, dataset_folder):
        """
        Delta format supports time travel. In order to support this 
        feature, older files version are kept inside dataset folder. 
        This method can be used to force a removal of these old versions.
        
        :params dataset_folder: Dataset's filepath 
        """
        size = sum(f.stat().st_size for f in Path(dataset_folder).glob('**/*') if f.is_file()) / (1024**3)
        self._logger(f"[INFO] Starting to remove old files.\nCurrent size: {size:.2f} GB")
        deltaTable = DeltaTable.forPath(self.spark_session, dataset_folder)
        t3 = time.time()
        deltaTable.vacuum(0)
        t4 = time.time()
        elapsed_time2 = int(t4-t3)
        size = sum(f.stat().st_size for f in Path(dataset_folder).glob('**/*') if f.is_file()) / (1024**3)
        self._logger(f"[INFO] Operation completed. It took {elapsed_time2} seconds to remove old files.\nNew size: {size:.2f} GB")


    def _logger(self, msg):
        """
        A dummy method for recording events on screen and ensuring their persistence in a file.
        """
        print(msg, flush=True)

        if self.LOG_OUTPUT:
            with open(self.LOG_OUTPUT, "a") as f:
                f.write(msg+"\n")    
        
    
    def _convert(self, input_filepath, output_filepath, filter_by_contry):
        """

        
        """
        df = self.spark_session.read.format("avro")\
            .option("mode", "PERMISSIVE")\
            .load(input_filepath)
            
        self._logger(f"[INFO] Schema inferred to all columns. Starting conversion.")

        if filter_by_contry:
            df = df.filter(F.col("location.country") == filter_by_contry)

        @F.udf
        def gen_ulid(timestamp_var):
            return ulid.from_timestamp(timestamp_var).str
        
        censys = df.withColumn("location", F.col("location").dropFields("registered_country").dropFields("registered_country_code").dropFields("postal_code").dropFields("timezone").dropFields("continent"))\
            .withColumn("autonomous_system", F.col("autonomous_system").dropFields("organization").dropFields("description"))\
            .withColumn("services", F.explode("services"))\
            .select(F.col("host_identifier.ipv4").alias("ip_str"), 
                    F.col("ipv4_int").alias("ip"), 
                    "location", 
                    "autonomous_system",
                    F.col("operating_system").alias("ip_operating_system"),
                    "services.*")\
            .withColumn("banner", F.base64("banner"))\
            .withColumn("banner_hashes", F.col("banner_hashes").getField(0))\
            .withColumnRenamed("observed_at", "timestamp")\
            .withColumnRenamed("labels", "banner_labels")\
            .withColumnRenamed("parsed_json", "data")\
            .withColumnRenamed("banner_hashes", "banner_hash")\
            .withColumn("year", F.year("timestamp"))\
            .withColumn("date", F.col("timestamp").cast("date"))\
            .withColumn("censys_id", gen_ulid("timestamp"))


        if not os.path.exists(output_filepath):
            censys.repartition(self.n_cores * 3, "year", "date")\
                .write.format("delta")\
                  .mode("append")\
                  .option("mergeSchema", "true")\
                  .partitionBy("year", "date")\
                  .save(output_filepath)  
        else:
    
            deltaTable = DeltaTable.forPath(self.spark_session, output_filepath)
            deltaTable.alias('old')\
              .merge(censys.alias('new'),
                     'old.ip = new.ip AND old.timestamp = new.timestamp AND old.source_ip = new.source_ip')\
              .whenNotMatchedInsertAll()\
              .execute()
 
     

