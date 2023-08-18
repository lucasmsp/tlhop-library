#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import shutil
from bs4 import BeautifulSoup
import os
import glob
from datetime import datetime
import urllib.request

class AS2Type(object):

    """
    AS2Type Dataset

    Inferred classified Autonomous Systems (ASes) by their business type.
    
    Reference: https://catalog.caida.org/dataset/as_classification
    """

    _ERROR_MESSAGE_000 = "[ERROR] This crawler requires an environment variable 'TLHOP_DATASETS_PATH' containing a folder path to be used as storage to all TLHOP datasets."
    _ERROR_MESSAGE_001 = ("[ERROR] Destination output already exists ('{}') but RELEASE file is " +
        "missing. Desconsidering previous files, if they exists.")
    _ERROR_MESSAGE_002 = "[ERROR] Mentioned file '{}' in RELEASE was not found."
    
    _INFO_MESSAGE_001 = "Last crawling timestamp: {}"
    _INFO_MESSAGE_002 = "A most recent version of the current dataset was found: {}"
    _INFO_MESSAGE_003 = "The current dataset version is the most recent."
    _INFO_MESSAGE_004 = "New dataset version is download with success!"
    _INFO_MESSAGE_005 = "Downloading file: '{}'\nAdvanced _INFO:\n{}"
    
    def __init__(self):
        """
        During the initialization process, it will check if a TLHOP directory folder is set and valid.
        It also will check on the Internet if an new dataset version is available to download in case 
        of a previous download was found during folder lookup.
        """
        self.download_url = "https://publicdata.caida.org/datasets/as-classification_restricted/"
        self.path = "caida-as2type/"
        self.expected_schema = {"ext": '.as2types.txt.gz', "outname": "as2type.csv"}
        self.new_version = None
        self.last_file = {}
    
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
        elif os.path.exists(self.basepath+"RELEASE"):
            with open(self.basepath+"RELEASE", "r") as f:
                url, timestamp, filename = f.read().split("\n")[0].split("|")
                if not os.path.exists(self.basepath+filename):
                    raise Exception(self._ERROR_MESSAGE_001.format(filename))
                else:
                    self.last_file = {"url": url, "timestamp": timestamp, "outname": filename}
                    print(self._INFO_MESSAGE_001.format(timestamp))
        else:
            print(self._ERROR_MESSAGE_001.format(self.basepath))
        
    def _check_for_new_files(self, download_url):
        response = requests.get(download_url)
        if response.ok:
            response_text = response.text
        else:
            return response.raise_for_status()
        
        soup = BeautifulSoup(response_text, 'html.parser')
        files = sorted([download_url + node.get('href') 
                        for node in soup.find_all('a') 
                        if node.get('href').endswith(self.expected_schema["ext"])])
        self.new_version = files[-1]
        
        if self.new_version != self.last_file.get("url", None):
            print(self._INFO_MESSAGE_002.format(self.new_version))
        else:
            print(self._INFO_MESSAGE_003)

    def describe(self):
        """
        Function to describe some informations about the dataset: 
        description, reference and fields.
        """
        print("""
        # AS2Type Dataset
        
        - Description: Inferred classified Autonomous Systems (ASes) by their business type.
        - Reference: https://catalog.caida.org/dataset/as_classification
        - Download link: {}
        - File format: <AS>|<Source>|<Class>
        """.format(self.download_url))

    def download(self):
        """
        Downloads a new dataset version available in its source link. 
        """

        info_stats = urllib.request.urlopen(self.new_version)
        print(self._INFO_MESSAGE_005.format(self.new_version, info_stats.info()))
        local_filename = self.basepath + self.new_version.split('/')[-1]
        with requests.get(self.new_version, stream=True) as r:
            with open(local_filename, 'wb') as f:
                shutil.copyfileobj(r.raw, f)

        outfile = self.basepath + self.expected_schema["outname"]
        os.system('zcat ' + local_filename + ' > ' + outfile)
        os.remove(local_filename)
        
        now = datetime.now().strftime("%Y%m%d_%H%M%S")
        msg = "{}|{}|{}".format(self.new_version, now, self.expected_schema["outname"])
        f = open(self.basepath+"RELEASE", "w")
        f.write(msg)
        f.close()
        print(self._INFO_MESSAGE_004)
        return True
