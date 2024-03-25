#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import shutil
from bs4 import BeautifulSoup
import os
import glob
from datetime import datetime
import urllib.request
import zipfile
import json 

import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql import SparkSession

class NISTNVD(object):

    """
    NIST NVD

    The National Vulnerability Database (NVD) is the U.S. government 
    repository of standards based vulnerability management data.
    
    Reference: https://nvd.nist.gov/
    """

    _ERROR_MESSAGE_000 = "[ERROR] This crawler requires an environment variable 'TLHOP_DATASETS_PATH' containing a folder path to be used as storage to all TLHOP datasets."
    _ERROR_MESSAGE_001 = ("[ERROR] Destination output already exists ('{}') but RELEASE file is " +
        "missing. Because of that, the crawler will not take in count previous files, if they exists.")
    _ERROR_MESSAGE_002 = "[ERROR] Mentioned file '{}' in RELEASE was not found."
    _ERROR_MESSAGE_003 = "None active Spark session was found. Please start a new Spark session before use DataSets API."
    
    _INFO_MESSAGE_001 = "[INFO] Last crawling timestamp: {}"
    _INFO_MESSAGE_002 = "[INFO] A most recent version of the current dataset was found."
    _INFO_MESSAGE_003 = "[INFO] The current dataset version is the most recent."
    _INFO_MESSAGE_004 = "[INFO] New dataset version is download with success!"
    _INFO_MESSAGE_005 = "[INFO] Downloading new file: '{}'"
    _INFO_MESSAGE_006 = "[INFO] Generating new consolidated file."
    
    def __init__(self):
        """
        During the initialization process, it will check if a TLHOP directory folder is set and valid.
        It also will check on the Internet if an new dataset version is available to download in case 
        of a previous download was found during folder lookup. Because the download data will be 
        pos-processed to an efficient format, the initialization process will also check if a Spark 
        session is already created.
        """

        self.download_url = "https://nvd.nist.gov/feeds/json/cve/1.1/nvdcve-1.1-{year}.json.zip"
        self.path = "nist-nvd/"
        self.expected_schema = {"outname": "cve_lib.gz.parquet"}
        self.new_version = False
        self.last_file = {}
        self.now = datetime.now()
        self.year = self.now.year

        self.spark_session = SparkSession.getActiveSession()
        if not self.spark_session:
            raise Exception(self._ERROR_MESSAGE_003)
    
        root_path = os.environ.get("TLHOP_DATASETS_PATH", None)
        if not root_path:
            raise Exception(self._ERROR_MESSAGE_000)
        self.root_path = (root_path+"/").replace("//", "/")
        self.basepath = self.root_path+self.path
        
        self._check_status()
        self._check_for_new_files(self.download_url)

    def _check_status(self):
        if not os.path.exists(self.basepath):
            os.mkdir(self.basepath)
            if not os.path.exists(self.basepath+"raw"):
                os.mkdir(self.basepath+"raw")
        elif os.path.exists(self.basepath+"RELEASE"):
            
            if not os.path.exists(self.basepath + self.expected_schema["outname"]):
                print(self._ERROR_MESSAGE_002.format(self.expected_schema["outname"]))
            
            with open(self.basepath+"RELEASE", "r") as f:
                for line in f.read().split("\n"):
                    url, etag, timestamp = line.split("|")
                    filename = url.split("/")[-1]
                    if not os.path.exists(self.basepath + "raw/" + filename):
                        raise Exception(self._ERROR_MESSAGE_001.format(filename))
                    self.last_file[url] = {"etag": etag, "timestamp": timestamp}
                print(self._INFO_MESSAGE_001.format(timestamp))
        else:
            print(self._ERROR_MESSAGE_001.format(self.basepath))
        
    def _check_for_new_files(self, download_url):
        
        found_update = False
        for y in range(2002, self.year+1):
            print(f"Checking CVES of year {y}")
            url = self.download_url.format(year=y)
            info = urllib.request.urlopen(url, timeout=30)
            etag = info.info()["ETag"]
            
            if url not in self.last_file:
                self.last_file[url] = {"etag": None}
                
            if etag != self.last_file[url]["etag"]:
                self.new_version = True
                self.last_file[url]["download"] = True
                self.last_file[url]["etag"] = etag
                self.last_file[url]["timestamp"] = self.now
            else:
                self.last_file[url]["download"] = False
        
        if self.new_version:
            print(self._INFO_MESSAGE_002)
        else:
            print(self._INFO_MESSAGE_003)

    def describe(self):
        """
        Function to describe some informations about the dataset: 
        description, reference and fields.
        """
        print("""
        # NIST NVD
        
        - Description: The National Vulnerability Database (NVD) is the U.S. government 
          repository of standards based vulnerability management data.
        - Reference: https://nvd.nist.gov/
        - Download link: {}
        - Fields: cve_id, description, cvssv2, cvssv3, publishedDate, lastModifiedDate, 
                  baseMetricV2, baseMetricV3, cpe, references, published_year, rank_cvss_v2,
                  rank_cvss_v3
        """.format(self.download_url))

    def download(self):
        """
        Downloads a new dataset version available in the source link. After download, it process the
        original data format to enrich its data and to convert to Parquet format.
        """
        
        if self.new_version:
            raw_directory = self.basepath + "raw/" 
            for y in range(2002, self.year+1):
                url = self.download_url.format(year=y)
                filename = url.split("/")[-1] 
                filepath = raw_directory + filename
                
                if self.last_file[url]["download"]:
                    print(self._INFO_MESSAGE_005.format(filename))
                    fin = requests.get(url, allow_redirects=True)
                    fout = open(filepath, 'wb')
                    fout.write(fin.content)
                    fout.close()
                    fin.close()
                
                with zipfile.ZipFile(filepath, 'r') as zip_ref:
                    zip_ref.extractall(raw_directory)
    
            outfile = self.basepath + self.expected_schema["outname"]
            self._process(raw_directory, outfile)
            
            for f in glob.glob(raw_directory+"*.json"):
                os.remove(f)
            
            now = self.now.strftime("%Y%m%d_%H%M%S")
            msg = ""
            for key, values in self.last_file.items():
                msg += "{}|{}|{}\n".format(key, values["etag"], now)
            msg = msg[0:-1]
            f = open(self.basepath+"RELEASE", "w")
            f.write(msg)
            f.close()
            print(self._INFO_MESSAGE_004)
        return True
    
    def _process(self, input_folder, outfile):

        print(self._INFO_MESSAGE_006)

        df = self.spark_session.read.option("multiline","true")\
            .option("mode", "PERMISSIVE")\
            .json(input_folder +"*.json")

        df = df.select(F.explode("CVE_Items").alias("CVE_Items"))\
            .select("CVE_Items.*")\
            .select("cve.CVE_data_meta.ID", 
                    F.col("cve.problemtype.problemtype_data").getItem(0).getField("description").getField("value").alias("cwe"),
                    F.concat_ws(",", F.col("cve.references.reference_data.url")).alias("references"), 
                    F.col("cve.description.description_data").alias("description"), 
                    F.col("configurations.nodes").alias("cpes"),
                    "impact.*", 
                    "lastModifiedDate", 
                    "publishedDate")\
            .withColumn("description", _get_description(F.col("description.lang"), F.col("description.value")))\
            .withColumn("description", F.trim(F.regexp_replace(F.col("description"), r'(;\n)', ',')))\
            .withColumn("publishedDate", F.to_date(F.unix_timestamp(F.col("publishedDate"), "yyyy-MM-dd'T'HH:mm'Z'").cast("timestamp")))\
            .withColumn("cvss_version", F.when(F.col("baseMetricV3.cvssV3.version").isNotNull(), F.col("baseMetricV3.cvssV3.version")).otherwise(F.col("baseMetricV2.cvssV2.version")))
        
        renamingV2 = [F.col('baseMetricV2.'+ col) for col in df.select('baseMetricV2.*').drop("cvssV2").columns] + \
                     [F.col('baseMetricV2.cvssV2.' + col).alias(col.replace("baseScore", "score")) for col in df.select('baseMetricV2.cvssV2.*').columns]
        renamingV3 = [F.col('baseMetricV3.'+ col) for col in df.select('baseMetricV3.*').drop("cvssV3").columns] + \
                     [F.col('baseMetricV3.cvssV3.' + col).alias(col.replace("baseScore", "score")) for col in df.select('baseMetricV3.cvssV3.*').columns]
        
        df = df.withColumn("cvss_v2", F.struct(*renamingV2))\
            .withColumn("cvss_v3", F.struct(*renamingV3))\
            .drop('baseMetricV2', 'baseMetricV3')
        
        df = df.withColumn("cvss_v2", df["cvss_v2"].withField('rank', _bucket_cvss_v2(F.col("cvss_v2.score"))))
        df = df.withColumn("cvss_v3", df["cvss_v3"].withField('rank', _bucket_cvss_v3(F.col("cvss_v3.score"))))
        df = df.withColumn("cvss_score", F.when(F.col("cvss_v3.score").isNotNull(), F.col("cvss_v3.score")).otherwise(F.col("cvss_v2.score")))

        df = df.select(F.col("ID").alias("cve_id"), 
                       'cvss_score',
                       'cvss_version',  
                       "description",     
                       "publishedDate", 
                       "lastModifiedDate", 
                       "cvss_v2", 
                       "cvss_v3", 
                       "cwe", 
                       "cpes", 
                       "references")
        
        df.coalesce(1).write.parquet(outfile+"_folder", compression="gzip")
        
        sparkfile = glob.glob(outfile+"_folder/part-*.parquet")[0]
        os.rename(sparkfile, outfile)
        shutil.rmtree(outfile+"_folder")
    
    
@F.udf
def _get_description(key, value):
    return value[key.index("en")]

@F.udf(returnType=StringType())
def _nesting_dict(row):
    if row:
        row = row.asDict()
        keys = [k  for k in row if isinstance(row[k], Row)]

        for k in keys:
            tmp = row[k].asDict()
            for k2 in tmp:
                row["{}_{}".format(k, k2)] = tmp[k2]
            del row[k]
        return json.dumps(row)
    else:
        return None

@F.udf
def _get_cpe(row):
    tmp = []
    if len(row)>0:
        row = row[0]
        for e in row:
            tmp.append(e[0])
    tmp = ",".join(tmp)
    return tmp

@F.udf
def _bucket_cvss_v3(score):
    if score:
        score = float(score)
        if score < 0.1:
            return "None"
        elif score < 4.0:
            return "low"
        elif score < 7.0:
            return "medium"
        elif score < 9.0:
            return "high"
        else: 
            return "critical"
    else:
        return "None"

@F.udf
def _bucket_cvss_v2(score):
    if score:
        score = float(score)
        if score < 4.0:
            return "low"
        elif score < 7.0:
            return "medium"
        else: 
            return "critical"
    else:
        return "low"
