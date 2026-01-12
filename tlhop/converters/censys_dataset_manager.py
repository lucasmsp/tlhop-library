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

from delta.tables import *

from tlhop.datasets import DataSets
from tlhop.library import *


class CensysDatasetManager(object):
    
    _ERROR_MESSAGE_001 = "None active Spark session was found. Please start a new Spark session before use the API."
    
    def __init__(self, filter_by_contry='Brazil', output_log=None):
        """
        A Class to convert Censys's Avro files into a single Delta format, to enrich and manage the dataset.
        
        :params filter_by_contry: Filter only records of a specific country (default, Brazil). If None, it will save the complete data; 
        :params output_log: A filepath to store the log execution (default None, only prints on screen).
        """
        
        self.spark_session = SparkSession.getActiveSession()
        if not self.spark_session:
            raise Exception(self._ERROR_MESSAGE_001)
            
        self.LOG_OUTPUT = output_log
        self.filter_by_contry = filter_by_contry
        
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

    def convert_dump_to_df(self, snapshot_input_folder):
        """
        Convert the original Avro snapshot into DataFrame without writing into a file.
        
        :params snapshot_input_folder: The snapshot folder;
        :return: A Spark's DataFrame
        """
        df = self._convert(snapshot_input_folder, persist=False, output_folder=None)
        
        return df
        
    def convert_files(self, snapshot_input_folder, output_folder):
        """
        Converts all Avro files in `snapshot_input_folder` into a single DELTA file format.
        
        :params snapshot_input_folder: The snapshot folder;
        :params output_folder: A path where the converted dataset will be stored;
        """
        
        filename = os.path.basename(snapshot_input_folder)
        self._logger("[INFO] Reading {} and saving as {}".format(filename, output_folder))

        t1 = time.time()
        self._convert(snapshot_input_folder, persist=True, output_folder=output_folder)
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
        deltaTable.optimize().executeZOrderBy("meta_id")
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
        
    
    def _convert(self, input_filepath, persist, output_folder=None):
        """
        :params snapshot_input_folder: The snapshot folder;
        :params output_folder: A path where the converted dataset will be stored;
        """
        df = self.spark_session.read.format("avro")\
            .option("mode", "PERMISSIVE")\
            .load(input_filepath)
            
        self._logger(f"[INFO] Schema inferred to all columns. Starting conversion.")

        if self.filter_by_contry:
            df = df.filter(F.col("location.country") == self.filter_by_contry)
        
        censys = df.withColumn("location", F.col("location").dropFields("registered_country").dropFields("registered_country_code").dropFields("postal_code").dropFields("timezone").dropFields("continent"))\
        .withColumn("autonomous_system", F.col("autonomous_system").dropFields("description"))\
        .withColumn("services", F.explode("services"))\
        .select(F.col("host_identifier.ipv4").alias("ip_str"),
                F.col("host_identifier.ipv6").alias("ipv6"),
                F.col("ipv4_int").alias("ip_int"), 
                "location", 
                "autonomous_system",
                F.col("operating_system").alias("ip_operating_system"),
                "services.*",
                F.col("snapshot_date").alias("snapshot_timestamp")
               )\
        .withColumn("ip", F.when(F.col("ip_str").isNotNull(), F.col("ip_str")).otherwise(F.col("ipv6")))\
        .withColumn("ip_info", F.struct("ip_int", "ip_str", "ipv6"))\
        .drop("ip_int", 'ip_str', "ipv6")\
        .withColumn("banner", F.decode("banner", "utf-8") )\
        .withColumn("banner_hashes", F.col("banner_hashes").getField(0))\
        .withColumnRenamed("observed_at", "timestamp")\
        .withColumnRenamed("labels", "tags")\
        .withColumnRenamed("parsed_json", "data")\
        .withColumnRenamed("banner_hashes", "banner_hash")\
        .withColumn("censys", F.struct("snapshot_timestamp", "perspective", "source_ip", "discovery_method"))\
        .drop("snapshot_timestamp", "perspective", "source_ip", "discovery_method")\
        .withColumn("year", F.year("timestamp"))\
        .withColumn("date", F.col("timestamp").cast("date"))\
        .withColumn("meta_id", gen_ulid("timestamp"))


        def decode_and_replace_binary(df, schema, prefix=""):
            new_cols = []
            for field in schema.fields:
                field_name = f"{prefix}.{field.name}" if prefix else field.name
                if isinstance(field.dataType, BinaryType):
                    # Substitui campo binário por texto decodificado
                    # print(f"Substituindo {field_name} por {field.name} (bruto)")
                    new_cols.append(F.decode(F.col(field_name), "utf-8").alias(field.name))
                elif isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, BinaryType):
                    # Aplica decode a cada elemento do array de binários
                    # print(f"Substituindo {field_name} por {field.name} (array)")
                    new_cols.append(F.transform(F.col(field_name), lambda x: F.decode(x, "utf-8")).alias(field.name))
                elif isinstance(field.dataType, StructType):
                    # Recria struct com campos internos processados
                    nested_cols = decode_and_replace_binary(df, field.dataType, field_name)
                    new_cols.append(F.struct(*nested_cols).alias(field.name))
                else:
                    new_cols.append(F.col(field_name))
            return new_cols
        
        new_columns = decode_and_replace_binary(censys, censys.schema)
        censys = censys.select(*new_columns)
        
        cols_to_json = ["http"]
        for c in cols_to_json:
            censys = censys.withColumn("http", F.when(F.col("http").isNotNull(), F.to_json("http")))
        
        censys = censys.withColumn("http", F.when(F.col("http") == '{"request":{},"response":{}}', F.lit(None)).otherwise(F.col("http")))
        
        
        first_order = ['year', 'date', 'timestamp', 'meta_id', 'ip', 'ip_info', "service_name", "extended_service_name", 'autonomous_system', 'censys',
                       'tags', 'location', 'ip_operating_system', 'port', 'transport', "banner", "data"]
        
        columns = [c for c in first_order if c in censys.columns]
        columns += [c for c in censys.columns if c not in first_order]
        censys = censys.select(*columns)

        if persist:
            if not os.path.exists(output_folder):
                censys.repartition(self.n_cores * 3, "year", "date")\
                    .write.format("delta")\
                      .mode("append")\
                      .option("userMetadata", input_filepath) \
                      .option("mergeSchema", "true")\
                      .partitionBy("year", "date")\
                      .save(output_folder)  
            else:
                self.spark_session.conf.set("spark.databricks.delta.commitInfo.userMetadata", input_filepath)
                deltaTable = DeltaTable.forPath(spark, output_folder)
                deltaTable.alias('old')\
                  .merge(censys.alias('new'),
                         'old.year = new.year AND old.date = new.date AND old.ip = new.ip AND old.timestamp = new.timestamp'
                        )\
                  .whenNotMatchedInsertAll()\
                  .execute()
    
        return censys
     

