#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import os
from datetime import datetime
import pandas as pd
import urllib.request
from pyspark.sql import SparkSession
import pyspark.sql.functions as F 
from delta.tables import *
import time
from deltalake import write_deltalake

class FirstEPSS(object):

    """
    FIRST's Exploit Prediction Scoring system (EPSS) 

    EPSS is a daily estimate of the probability of exploitation activity being observed over the next 30 days. 
    
    Reference: https://www.first.org/epss/
    """

    _ERROR_MESSAGE_000 = "None active Spark session was found. Please start a new Spark session before use DataSets API."
    _ERROR_MESSAGE_001 = "[ERROR] This crawler requires an environment variable 'TLHOP_DATASETS_PATH' containing a folder path to be used as storage to all TLHOP datasets."
    _ERROR_MESSAGE_002 = ("[ERROR] Destination output already exists ('{}') but RELEASE file is " +
        "missing. Desconsidering previous files, if they exists.")
    _ERROR_MESSAGE_003 = "[ERROR] Mentioned file '{}' in RELEASE was not found."
    _ERROR_MESSAGE_004 = "[ERROR] File in url '{}' could not be downloaded!"
    
    _INFO_MESSAGE_001 = "Last crawling timestamp: {}"
    _INFO_MESSAGE_002 = "A most recent version of the current dataset was found."
    _INFO_MESSAGE_003 = "The current dataset version is the most recent."
    _INFO_MESSAGE_004 = "Downloading new file ..."
    _INFO_MESSAGE_005 = "New dataset version is download with success!"
    
    def __init__(self):
        """
        During the initialization process, it will check if a TLHOP directory folder is set and valid.
        It also will check on the Internet if an new dataset version is available to download in case 
        of a previous download was found during folder lookup.
        """
        
        self.download_url = "https://epss.cyentia.com/epss_scores-YYYY-mm-dd.csv.gz"
        self.path = "first-epss/"
        self.expected_schema = {"outname": "epss.delta"}
        self.new_version = None
        self.last_file = {"timestamp": "2021-04-14"}
        self.date_format = "%Y-%m-%d"
        self.current_day = datetime.now().strftime(self.date_format)
        
        self.spark_session = SparkSession.getActiveSession()
        if not self.spark_session:
            raise Exception(self._ERROR_MESSAGE_000)
    
    
        root_path = os.environ.get("TLHOP_DATASETS_PATH", None)
        if not root_path:
            raise Exception(self._ERROR_MESSAGE_001)
        self.root_path = (root_path+"/").replace("//", "/")
        self.basepath = self.root_path+self.path
        
        self._check_status()
        self._check_for_new_files(self.download_url)

    def _check_status(self):
        if not os.path.exists(self.basepath):
            os.mkdir(self.basepath)

        elif os.path.exists(self.basepath+"RELEASE"):
            with open(self.basepath+"RELEASE", "r") as f:
                timestamp = f.read().split("\n")[0]
                if not os.path.exists(self.basepath+self.expected_schema["outname"]):
                    raise Exception(self._ERROR_MESSAGE_002.format(self.expected_schema["outname"]))
                else:
                    self.last_file = {"timestamp": timestamp}
                    print(self._INFO_MESSAGE_001.format(timestamp))
        else:
            print(self._ERROR_MESSAGE_002.format(self.basepath))
        
    def _check_for_new_files(self, download_url):

        if self.last_file["timestamp"] == self.current_day:
            self.new_version = False
            print(self._INFO_MESSAGE_003)
        elif urllib.request.urlopen(download_url.replace("YYYY-mm-dd", self.current_day)):
            print(self._INFO_MESSAGE_002)
            self.new_version = True
        else:
            self.new_version = False
            print(self._INFO_MESSAGE_003)
            
    def describe(self):
        """
        Function to describe some informations about the dataset: 
        description, reference and fields.
        """
        print("""
        # FIRST's Exploit Prediction Scoring system (EPSS) 
        
        - Description: EPSS is a daily estimate of the probability of exploitation activity being observed over the next 30 days. 
        - Reference: https://www.first.org/epss/
        - Download link: {}
        """.format(self.download_url))

    def _download_dataset(self, url):
        retry = 3
        tmp_df = None
        
        while retry > 0:
            try:
                tmp_df = pd.read_csv(url)
                time.sleep(3)
                break
            except:
                pass
            retry-=1
            

        if retry == 0:
            print (self._ERROR_MESSAGE_004.format(url))

        return tmp_df

    def _gen_release(self, current_day):

        msg = "{}".format(current_day)
        with open(self.basepath+"RELEASE", "w") as f:
            f.write(msg)

    def _optimize_delta(self, output_path):
        # Setting Delta configurations
        minFileSize = 200 * 1024 * 1024
        maxFileSize = 256 * 1024 * 1024
        maxThreads = 10
        
        self.spark_session.conf.set("spark.databricks.delta.optimize.minFileSize", minFileSize)
        self.spark_session.conf.set("spark.databricks.delta.optimize.maxFileSize", maxFileSize)
        self.spark_session.conf.set("spark.databricks.delta.optimize.maxThreads", maxThreads)
        self.spark_session.conf.set("spark.databricks.delta.retentionDurationCheck.enabled","false")
        self.spark_session.conf.set("spark.databricks.delta.merge.repartitionBeforeWrite.enabled", "true") 
        
        deltaTable = DeltaTable.forPath(self.spark_session, output_path)  # For path-based tables
        deltaTable.optimize().executeCompaction()
        deltaTable.vacuum(0)
        
    def _download_single_file(self, day_str):

        day = datetime.strptime(day_str, self.date_format)
        current_url = self.download_url.replace("YYYY-mm-dd", day_str)
        
        tmp_df = self._download_dataset(current_url)
        df = None
        
        if isinstance(tmp_df, pd.DataFrame):

            # model v1
            if  day < datetime.strptime("2022-02-03", self.date_format):
                tmp_df = tmp_df.rename(columns={'cve': 'cve_id'}) 
                tmp_df["model_version"] =  "v2021-04-14"
                tmp_df['epss'] = tmp_df['epss'].astype(float)
                
                cols = ["cve_id", "score_date", "year", "model_version", "epss"]
                if "percentile" in tmp_df.columns:
                    cols += ["percentile"]
                    tmp_df['percentile'] = tmp_df['percentile'].astype(float)

                tmp_df["score_date"] =  day   
                tmp_df['year'] = tmp_df['score_date'].dt.year
                tmp_df['score_date'] = pd.to_datetime(tmp_df['score_date']).dt.date
                tmp_df['year'] = tmp_df['year'].astype('int32')
                
                df = tmp_df[cols]

            else:
                info = tmp_df.iloc[0].index.tolist()
                tmp_df = tmp_df.drop(["cve"], axis=0)\
                    .reset_index()
                tmp_df.columns = ["cve_id", "epss", "percentile"]
                
                tmp_df["model_version"] =  info[0][15:]
                tmp_df['epss'] = tmp_df['epss'].astype(float)
                tmp_df['percentile'] = tmp_df['percentile'].astype(float)
                
                tmp_df["score_date"] =  datetime.strptime(info[1][11:-14], self.date_format)
                tmp_df['year'] = tmp_df['score_date'].dt.year
                tmp_df['score_date'] = pd.to_datetime(tmp_df['score_date']).dt.date
                tmp_df['year'] = tmp_df['year'].astype('int32')
                
                df = tmp_df[["cve_id", "score_date", "year", "model_version", "epss", "percentile"]]

        return df  

    def download_to_df(self, day_str):
        tmp_df = self._download_single_file(day_str)
        return self.spark_session.createDataFrame(tmp_df)

    def download(self):
        """
        Downloads a new dataset version available in its source link. 
        """
        if self.new_version:
            print(self._INFO_MESSAGE_004)
            previous_date = datetime.strptime(self.last_file["timestamp"], self.date_format)
            next_dates = pd.date_range(previous_date, pd.to_datetime('today'), freq='d')
            output_path = self.basepath + self.expected_schema["outname"]
            
            for day in next_dates:
                day_str = day.strftime(self.date_format)
                df = self._download_single_file(day_str)

                if isinstance(df, pd.DataFrame):
                    write_deltalake(output_path, df, mode='append', partition_by=['year'], overwrite_schema=True)                
                    self._gen_release(day_str)

            self._optimize_delta(output_path)

            print(self._INFO_MESSAGE_005)
        else:
            print(self._INFO_MESSAGE_003)
        return True
