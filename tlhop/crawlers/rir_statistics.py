#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import shutil
import os
import glob
from datetime import datetime, timedelta
import pandas as pd
import ipaddress
from pyspark.sql import SparkSession
import pyspark.sql.functions as F 
#from delta.tables import *
from deltalake import write_deltalake, DeltaTable

class LACNICStatistics(object):

    """
    LACNIC RIR Statistics

    This dataset contains daily summary reports of the allocations and assignments of numeric Internet address resources within
    ranges originally delegated to LACNIC and historical ranges
    transferred to LACNIC by other registries.
    
    Reference: https://ftp.lacnic.net/pub/stats/lacnic/
    """

    
    _ERROR_MESSAGE_000 = "[ERROR] This crawler requires an environment variable 'TLHOP_DATASETS_PATH' containing a folder path to be used as storage to all TLHOP datasets."
    _ERROR_MESSAGE_001 = ("[ERROR] Destination output already exists ('{}') but RELEASE file is " +
        "missing. Desconsidering previous files, if they exists.")
    _ERROR_MESSAGE_002 = "[ERROR] Mentioned file '{}' in RELEASE was not found."
    #_ERROR_MESSAGE_003 = "[ERROR] None active Spark session was found. Please start a new Spark session before use DataSets API."
    
    _INFO_MESSAGE_001 = "Last crawling timestamp: {}"
    _INFO_MESSAGE_002 = "A most recent version of the current dataset was found."
    _INFO_MESSAGE_003 = "The current dataset version is the most recent."
    _INFO_MESSAGE_004 = "New dataset version is download with success!"
    _INFO_MESSAGE_005 = "Downloading new file ..."
    
    def __init__(self):
        """
        During the initialization process, it will check if a TLHOP directory folder is set and valid.
        It also will check on the Internet if an new dataset version is available to download in case 
        of a previous download was found during folder lookup.
        """
        self.download_url = "https://ftp.lacnic.net/pub/stats/lacnic/delegated-lacnic-{date}"
        self.path = "rir_statistics/"
        self.expected_schema = {"outname": "rir_statistcs.delta"}
        self.new_version = False
        self.now = datetime.now()
        self.now_str = self.now.strftime("%Y%m%d")
        self.last_file = {}

        # self.spark_session = SparkSession.getActiveSession()
        # if not self.spark_session:
        #     raise Exception(self._ERROR_MESSAGE_003)
    
        root_path = os.environ.get("TLHOP_DATASETS_PATH", None)
        if not root_path:
            raise Exception(self._ERROR_MESSAGE_000)
        self.root_path = (root_path+"/").replace("//", "/")
        self.basepath = self.root_path+self.path
        
        self._check_status()
        self._check_for_new_files()

    def _check_status(self):
        self.last_file = {"date": '20210101', "outname": self.expected_schema['outname']}
        
        if not os.path.exists(self.basepath):
            os.mkdir(self.basepath)
            
        elif os.path.exists(self.basepath+"RELEASE"):
            with open(self.basepath+"RELEASE", "r") as f:
                last_date, filename = f.read().split("\n")[0].split("|")
                if not os.path.exists(self.basepath+filename):
                    raise Exception(self._ERROR_MESSAGE_001.format(filename))
                else:
                    self.last_file = {"date": last_date, "outname": filename}
                    print(self._INFO_MESSAGE_001.format(last_date))
        else:
            print(self._ERROR_MESSAGE_001.format(self.basepath))
        
    def _check_for_new_files(self):
        latest = datetime.strptime(self.last_file['date'], "%Y%m%d")
        
        if abs(self.now - latest).days > 1:
            print(self._INFO_MESSAGE_002)
            self.new_version = True
        else:
            print(self._INFO_MESSAGE_003)
            self.new_version = False

    def describe(self):
        """
        Function to describe some informations about the dataset: 
        description, reference and fields.
        """
        print("""
        # LACNIC RIR Statistics
        
        - Description: This dataset contains daily summary reports of the allocations and assignments of numeric Internet address resources within
                        ranges originally delegated to LACNIC and historical ranges
                        transferred to LACNIC by other registries.
        - Reference: https://ftp.lacnic.net/pub/stats/lacnic/RIR-Statistics-Exchange-Format.txt
        - Download link: {}
        """.format(self.download_url))

    def download(self):
        """
        Downloads a new dataset version available in its source link. 
        """
        if self.new_version:
            print(self._INFO_MESSAGE_005)
            previous_date = datetime.strptime(self.last_file['date'], "%Y%m%d")
            output_path = self.basepath + self.expected_schema['outname']
            
            def convert_begining_legth_to_range(t, start_ip_str, n_ips):
                start_ip_int, end_ip_int = None, None
                if t == 'ipv4':
                    start_ip_int = int(ipaddress.IPv4Network(start_ip_str)[0])
                    end_ip_int = start_ip_int + n_ips - 1
                return start_ip_int, end_ip_int
                
            
            for day in pd.date_range(previous_date, self.now - timedelta(days=1), freq='d'):
                day_str = day.strftime("%Y%m%d")
                df = pd.read_csv(self.download_url.format(date=day_str), 
                                 sep='|', skiprows=3, header=0, names=['registry', "country", "type", 'mask', "n_ips", "date", "status"])
                df[['start_ip_int', 'end_ip_int']] = df.apply(lambda x: convert_begining_legth_to_range(x['type'], x['mask'], x['n_ips']), axis=1, result_type='expand')
                df['crawler_date'] = day.date()
                df['crawler_year'] = day.date().year

                write_deltalake(output_path, df, mode='append', partition_by=['crawler_year'])
    
                self._gen_release(day_str)
                
            self._optimize_delta(output_path)
            print(self._INFO_MESSAGE_004)
        else:
            print(self._INFO_MESSAGE_003)
        return True

    def _gen_release(self, current_day):
        msg = "{}|{}".format(current_day, self.expected_schema["outname"])
        with open(self.basepath+"RELEASE", "w") as f:
            f.write(msg)

    def _optimize_delta(self, output_path):

        dt = DeltaTable(output_path)
        dt.optimize.compact(max_concurrent_tasks=10)
        dt.vacuum(retention_hours=0, dry_run=False, enforce_retention_duration=False)
