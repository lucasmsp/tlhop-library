#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import shutil
from bs4 import BeautifulSoup
import os
import glob
from datetime import datetime
import urllib.request
import json
import subprocess


class BrazilianCities(object):
    
    """
    Brazilian Cities

    This dataset is a compilation of several publicly available information about Brazilian Municipalities. Altought this
    dataset be public, is necessary a kaggle account to download it.
    
    Reference: https://www.kaggle.com/datasets/crisparada/brazilian-cities    
    """

    _ERROR_MESSAGE_000 = "[ERROR] This crawler requires an environment variable 'TLHOP_DATASETS_PATH' containing a folder path to be used as storage to all TLHOP datasets."
    _ERROR_MESSAGE_001 = ("[ERROR] Destination output already exists ('{}') but RELEASE file is " +
        "missing. Desconsidering previous files, if they exists.")
    _ERROR_MESSAGE_002 = "[ERROR] Mentioned file '{}' in RELEASE was not found."
    _ERROR_MESSAGE_003 = "[ERROR] Only 'manual' or 'tlhop' mode are supported."
    _ERROR_MESSAGE_004 = "[ERROR] This method requires kaggle API (`https://www.kaggle.com/docs/api`) to "\
                "download automatically."
    
    _INFO_MESSAGE_002 = "There is a new dataset version, created at {}"
    _INFO_MESSAGE_003 = "The current dataset version is the most recent."
    _INFO_MESSAGE_004 = "New dataset version added with success!"
    _INFO_MESSAGE_005 = "By default, the download of this dataset is expected to be manual."\
                  "In this mode, the user must download this dataset using the reference page "\
                  "(https://www.kaggle.com/datasets/crisparada/brazilian-cities), and place it "\
                  "as file '{}'. Please press [y/n] when the new version is already in the directory "\
                  "or to abort: "
    _INFO_MESSAGE_006 = "It seem that the dataset was not updated."
    
    def __init__(self):
        """
        During the initialization process, it will check if a TLHOP directory folder is set and valid.
        It also will check on the Internet if an new dataset version is available to download in case 
        of a previous download was found during folder lookup.
        """
        self.download_url = 'https://www.kaggle.com/datasets/crisparada/brazilian-cities'
        self.path = "brazilian-cities/"
        self.expected_schema = {"outname": "brazilian_cities.csv"}
        self.new_version = None
        self.last_file = {}
    
        root_path = os.environ.get("TLHOP_DATASETS_PATH", None)
        if not root_path:
            raise Exception(self._ERROR_MESSAGE_000)
        self.root_path = (root_path+"/").replace("//", "/")
        self.basepath = self.root_path+self.path
                
        self._check_status()
        self._check_for_new_files()

    def _check_status(self):
        if not os.path.exists(self.basepath):
            os.mkdir(self.basepath)
        elif os.path.exists(self.basepath+"RELEASE"):
            with open(self.basepath+"RELEASE", "r") as f:
                version, hash_md5 = f.read().split("\n")[0].split("|")
                if not os.path.exists(self.basepath+self.expected_schema['outname']):
                    raise Exception(self._ERROR_MESSAGE_001.format(self.expected_schema['outname']))
                else:
                    self.last_file = {"version": version, "hash_md5": hash_md5}
        else:
            print(self._ERROR_MESSAGE_001.format(self.basepath))
        
    def _check_for_new_files(self):
        res = requests.get(self.download_url)
        soup = BeautifulSoup(res.text, 'html.parser')
        content = json.loads(soup.head.find("script", type="application/ld+json").text)
        self.new_version = content["dateModified"]
        
        if self.new_version != self.last_file.get("version", None):
            print(self._INFO_MESSAGE_002.format(self.new_version))
        else:
            print(self._INFO_MESSAGE_003)

    def describe(self):
        """
        Function to describe some informations about the dataset: 
        description, reference, fields, authors and license.
        """
        print("""
        # Brazilian Cities
        
        - Description: This dataset is a compilation of several publicly available information about Brazilian Municipalities.
        - Reference: https://www.kaggle.com/datasets/crisparada/brazilian-cities
        - Fields: Each city contains 79 fields, please check reference page to further details.
        - Authors: All credits to Cristiana Parada (https://www.kaggle.com/crisparada).
        - License: This dataset is under CC BY-SA 4.0 License, which means that you are allowed to copy and redistribute the material in any medium or format.
        """)

    def download(self, mode="manual"):
        """
        Downloads a new dataset version available in its source link. 

        :params mode: Option to inform if you want to download its dataset
         manualy ("manual") or automatically ("tlhop"). When using "manual" mode, 
         the user must download this dataset using the reference page "\
         "(https://www.kaggle.com/datasets/crisparada/brazilian-cities), and place it "\
         "as file in '$TLHOP_DATASETS_PATH/brazilian-cities/brazilian_cities.csv'.
        """
        if mode not in ["manual", "tlhop"]:
              raise Exception(self._ERROR_MESSAGE_003)
        
        new_file = self.basepath + self.expected_schema["outname"]
        if mode == "manual":
            response = ""
            while response not in ["y", "n"]:
                response = input(self._INFO_MESSAGE_005.format(new_file))
            if response == "n":
                return False
            
        else:
            raise Exception("Not implemented yet!")
              
        new_md5_hash = subprocess.check_output("md5sum {}".format(new_file), shell=True)\
                  .decode()\
                  .split(" ")[0]
        
        if new_md5_hash != self.last_file.get("hash_md5", None):
            msg = "{}|{}".format(self.new_version, new_md5_hash)
            f = open(self.basepath+"RELEASE", "w")
            f.write(msg)
            f.close()
            print(self._INFO_MESSAGE_004)
            return True
        else:
            print(self._INFO_MESSAGE_006)
            return False