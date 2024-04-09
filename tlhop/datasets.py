#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
import pyspark.sql.functions as F                                                                              
from pyspark.sql.types import *
from pyspark.sql.window import Window

from pathlib import Path
from datetime import datetime, timezone
import builtins
import os
import sys
import glob
import pandas as pd
import numpy as np

from tlhop.library import print_full, normalize_string
from tlhop.schemas import Schemas
import tlhop.crawlers as crawlers
# TODO: Referer its crawlers

class DataSets(object):
    """
    DataSets API.
    """
    
    _INTERNAL_DATASET_LIST = {
        "HTTP_STATUS_FILE": { 
            "path": "tlhop_internal_datasets/http-codes/http-response-status.csv",
            "description": "Hypertext Transfer Protocol (HTTP) Status Code Registry",
            "method": "_read_http_status"
        },
        "ISO_639_LANGUAGE": { 
            "path": "tlhop_internal_datasets/language-codes/iso_639_language.csv",
            "description": "Language code (iso 639) mapping list",
            "method": "_read_language_code_lib"
        },
        "ACCENTED_WORDS_PT_BR_FILE": { 
            "path": "tlhop_internal_datasets/brazil-dictionaries/accented_words_ptbr.json",
            "description": "List of accented pt-br words",
            "method": "_read_ptbr_accented_words"
        },
        "PT_BR_DICTIONARY": { 
            "path": "tlhop_internal_datasets/brazil-dictionaries/portuguese-brazil.dic.txt",
            "description": "List of pt-br words with length more than 2 characteres",
            "method": "_read_ptbr_dictionary"
        },
        "UTF8_MAPPING_FILE": { 
            "path": "tlhop_internal_datasets/utf-codes/mapping-utf8-characters.csv",
            "description": "UTF-8 Code mapping",
            "method": "_read_utf8_mapping_file"
        }
    }
    
    _EXTERNAL_DATASET_LIST = {
        "NVD_CVE_LIB": { 
            "path": "nist-nvd/cve_lib.gz.parquet",
            "description": "NIST National Vulnerability Database",
            "method": "_read_nvd_cve_lib",
            "crawler": "NISTNVD"
        },
        "CISA_EXPLOITS": { 
            "path": "cisa-known-exploits/known_exploited_vulnerabilities.csv",
            "description": "CISA's Known Exploited Vulnerabilities Catalog",
            "method": "_read_cisa_known_exploits",
            "crawler": "CISAKnownExploits"
        },
        "AS_RANK_FILE": { 
            "path": "caida-rank/as_rank.csv",
            "description": "CAIDA's AS Rank",
            "method": "_read_as_rank",
            "crawler": "ASRank"
        },
        "AS_TYPE_FILE": { 
            "path": "caida-as2type/as2type.csv",
            "description": "CAIDA's AS Classification",
            "method": "_read_as_type",
            "crawler": "AS2Type"
        },
        "MIKROTIK_OS": { 
            "path": "mikrotik-releases/mikrotik_releases.csv",
            "description": "Mikrotik Operational System releases",
            "method": "_read_mikrotik_os",
            "crawler": "MikrotikReleases"
        },
        "BRAZILIAN_CITIES": { 
            "path": "brazilian-cities/brazilian_cities.csv",
            "description": "Brazil's cities information dataset",
            "method": "_read_brazil_cities",
            "crawler": "BrazilianCities"
        },
        "COLUMNS_MODULE_FILE": { 
            "path": "shodan-columns/columns_by_module.csv",
            "description": "Shodan's Module list and their columns frequency",
            "method": "_read_shodan_modules"
        },
        "ENDOFLIFE": { 
            "path": "endoflife/endoflife.json",
            "description": "Keep track of various End of Life dates and support lifecycles for various products",
            "method": "_read_eol"
        },
        "BRAZILIAN_IPS": { 
            "path": "brazil-ips/BrazilianIPs.csv",
            "description": "Lisf of Range of IPs available in Brazil's Internet",
            "method": "_read_brazilian_ip_range"
        },
        "BRAZILIAN_RF": {
            "path": "brazilian-rf/brazilian-rf-consolidated.gz.delta",
            "description": "Brazilian National Register of Legal Entities - CNPJ",
            "method": "_read_brazilian_rf"
        },
        "RDAP/TREE": {
            "path": "rdap/scan_<>/interval_tree.pickle",
            "description": "A interval tree generated during RDAP dataset creation.",
            "method": "_read_rdap_interval_tree"
        },
        "RDAP/DATASET": {
            "path": "rdap/scan_<>/rdap.parquet",
            "description": "RDAP dataset generated from a list of IPs",
            "method": "_read_rdap_dataset"
        },
        "FIRST_EPSS": {
            "path": "first-epss/epss.delta",
            "description": "FIRST's Exploit Prediction Scoring system (EPSS)",
            "method": "_read_epss_dataset"
        },
        "LACNIC_STATISTICS": {
            "path": "rir_statistics/rir_statistcs.delta",
            "description": "LACNIC RIR Statistics",
            "method": "_read_rir_dataset"
        }
        
    }
        
    _ERROR_MESSAGE_001 = "[ERROR] None active Spark session was found. Please start a new Spark session before use DataSets API."
    _ERROR_MESSAGE_002 = "[ERROR] TLHOP DataSets API requires an environment variable 'TLHOP_DATASETS_PATH' containing a folder path to be used as storage to all datasets."
    _ERROR_MESSAGE_003 = "[ERROR] Dataset code does not exists. Please run `Dataset().list_datasets()` to check the right code."
    _ERROR_MESSAGE_004 = "[ERROR] File or directory not found. Please check the informed path."
    
    _WARN_MESSAGE_001 = "[WARN] The following {} datasets remains missing: {}"
    _WARN_MESSAGE_002 = "[WARN] Checking updation is not supported yet to this dataset."  
    
    def __init__(self):
        """
        During the initialization process, it will try to conect to 
        an active Spark session and check if a TLHOP directory folder is set and 
        also valid.
        """
        
        self.spark_session = SparkSession.getActiveSession()
        if not self.spark_session:
            self.spark_session = SparkSession.builder.getOrCreate()
            if not self.spark_session:
                raise Exception(self._ERROR_MESSAGE_001)

        path = os.environ.get("TLHOP_DATASETS_PATH", None)
        if not path:
            raise Exception(self._ERROR_MESSAGE_002)
        self.path = (path+"/").replace("//", "/")
        
        self._DATASET_LIST = {**self._INTERNAL_DATASET_LIST, **self._EXTERNAL_DATASET_LIST}
        self.datasets_df = self._check_datasets()
        self.schemas = Schemas()
        
    def list_datasets(self):
        """
        Method to list all downloaded datasets information.
        """
        only_downloaded = self.datasets_df[self.datasets_df["Downloaded"]]
        print_full(only_downloaded)

    
    def list_all_supported_datasets(self):
        """
        Method that list of available datasets supported by TLHOP.

        :returns: NONE
        :rtype: NONE
        """
        
        missing = self.datasets_df[self.datasets_df["Downloaded"] == False]
        n_missing = len(missing)
        
        print_full(self.datasets_df)
        
        if n_missing > 0:
            color_bold_red = "\x1b[31;1m"
            color_reset = "\x1b[0m"
            missing_names = missing['Code name'].values.tolist()
            print(color_bold_red +self._WARN_MESSAGE_001.format(n_missing, missing_names) + color_reset)


    def _check_datasets(self):
        infos = []
        for code in sorted(list(self._DATASET_LIST.keys())):
            basepath = self._DATASET_LIST[code]["path"]
            
            if code in self._INTERNAL_DATASET_LIST:
                category = "internal"
                filepath = os.path.dirname(os.path.abspath(__file__)) + "/" + basepath
            else:
                filepath = self.path + basepath
                category = "external"

            description = self._DATASET_LIST[code]["description"]
            
            if "<>" in filepath:
                # this means that the dataset is inside an subfolder that changes over time
                RELEASE_FILE = os.path.split(os.path.split(filepath)[0])[0] + "/RELEASE"
                if os.path.exists(RELEASE_FILE):
                    with open(RELEASE_FILE, "r") as f:
                        info_release = f.readlines()[0].split("|")
                        timestamp = info_release[-1].replace("\n", "")
                        filepath = filepath.replace("<>", timestamp)    

            
            if os.path.exists(filepath):
                p = Path(filepath)
                if os.path.isdir(filepath):
                    size = sum(f.stat().st_size for f in p.glob('**/*') if f.is_file())
                else:
                    size = p.stat().st_size
                    
                size = round(size/float(1<<20), 2)
                timestamp = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc).strftime("%m/%d/%Y, %H:%M:%S")
                downloaded = True
            else:
                size = None
                timestamp = None
                downloaded = False

            
            infos.append([code,  description, category, downloaded, size, timestamp])

        return pd.DataFrame(infos, columns=["Code name", "Description", "Type", "Downloaded", "Size (MB)", "Last timestamp"])
        
    def read_dataset(self, code, check_update=False):
        """
        Method to read a dataset based on its reference code. 
        It returns a dataset as a Spark's DataFrame.

        :param code: DataSet code listed in `list_datasets` table;
        :param check_update: Option to check and update the dataset, if a new version is available (only available to few datasets);
        
        :returns: Data Frame
        :rtype: Spark's DataFrame.
        """
        if code not in self._DATASET_LIST:
            raise Exception(self._ERROR_MESSAGE_003)

        if code in self._INTERNAL_DATASET_LIST:
            _library_path = os.path.dirname(os.path.abspath(__file__))
            path = _library_path + "/" + self._DATASET_LIST[code]["path"]
        else:
            path = self.path + self._DATASET_LIST[code]["path"]
        
        if "<>" in path:
            # this means that the dataset is inside an subfolder that changes over time
            RELEASE_FILE = os.path.split(os.path.split(path)[0])[0] + "/RELEASE"
            if os.path.exists(RELEASE_FILE):
                with open(RELEASE_FILE, "r") as f:
                    info_release = f.readlines()[0].split("|")
                    timestamp = info_release[-1].replace("\n", "")
                    path = path.replace("<>", timestamp) 
                        
        if not os.path.exists(path):
            raise Exception(self._ERROR_MESSAGE_004)
            
        method = getattr(self, self._DATASET_LIST[code]["method"])

        if not check_update:
            df = method(path)
        elif code not in ["FIRST_EPSS", "LACNIC_STATISTICS", "NVD_CVE_LIB"]:
            print(self._WARN_MESSAGE_002)
            df = method(path)
        else:
            df = method(path, check_update=True)
        
        return df
    
    
    def _read_nvd_cve_lib(self, path, check_update=False):

        if check_update:
            crawler = crawlers.NISTNVD()
            crawler.download()
        
        cve_lib = self.spark_session.read.parquet(path, compression="gzip")
        
        return cve_lib
    
    def _read_cisa_known_exploits(self, path):
        cisa_lib = self.spark_session.read.csv(path, sep=";", header=True)\
            .withColumnRenamed("cveID", "cve_id")\
            .persist()
        
        return cisa_lib
    
    def _read_http_status(self, path):
        status_http =  self.spark_session.read.csv(path, sep=";", header=True)\
            .persist()
        return status_http
    
    def _read_utf8_mapping_file(self, path):
        mapping_utf8 = self.spark_session.read.csv(path, sep=";", quote='\"',header=True)\
            .persist()
        return mapping_utf8
    
    def _read_as_rank(self, path):
        as_rank = self.spark_session.read.csv(path, sep=";", header=True)\
            .withColumn("asn", F.concat(F.lit("AS"), F.col("asn")))\
            .withColumn("rank", F.col("rank").cast("int"))\
            .persist()
        return as_rank
    
    
    def _read_as_type(self, path):
        asn_type = self.spark_session.read.csv(path, sep="|", comment="#")\
            .withColumnRenamed("_c0", "asn")\
            .withColumnRenamed("_c1", "class")\
            .withColumnRenamed("_c2", "type")\
            .withColumn("asn", F.concat(F.lit("AS"), F.col("asn")))\
            .persist()
        return asn_type
    
    def _read_brazil_cities(self, path):
        brazil_cities = self.spark_session.read.csv(path, sep=",", header=True)\
            .withColumn("LAT", F.col("LAT").cast("double"))\
            .withColumn("LONG", F.col("LONG").cast("double"))\
            .withColumn("CITY", normalize_string(F.upper("CITY")))\
            .persist()

        return brazil_cities
    
    def _read_shodan_modules(self, path):
        shodan_module = self.spark_session.read.csv(path, header=True)\
            .persist()
        return shodan_module
    
    
    def _read_mikrotik_os(self, path):
        mikrotik_os = self.spark_session.read.csv(path, sep=";", header=True)\
            .persist()
        return mikrotik_os
    
    def _read_language_code_lib(self, path):
        iso_lang = self.spark_session.read.csv(path, sep=",", header=True)\
            .persist()
        return iso_lang
    
    def _read_ptbr_dictionary(self, path):
        pt_br = self.spark_session.read.csv(path, sep=",", header=False)\
            .withColumnRenamed("_c0", "word")\
            .persist()
        return pt_br
    
    def _read_ptbr_accented_words(self, path):
        accented_dict = self.spark_session.read.json(path)\
            .select(F.explode("words").alias("word"))\
            .persist()
        return accented_dict
    
    def _read_brazilian_ip_range(self, path):
        brazilianIPs = self.spark_session.read.csv(path, header=True)\
            .select(F.col("start_ip_int").cast("long"), F.col("end_ip_int").cast("long"),
                    "start_ip_str", "end_ip_str", F.col("n_ips").cast("long"))\
            .persist()
        return brazilianIPs
    
    def _read_eol(self, path):
        dd = pd.read_json(path, lines=True)
        dd.loc[dd["eol"] == False, "eol"] = None
        dd['eol'] = np.where(dd["eol"] == True, dd["latestReleaseDate"], dd["eol"])

        eol_ds = self.spark_session.createDataFrame(dd[["cycle", "releaseDate", "eol", "latest", "latestReleaseDate", "product"]])\
            .withColumn("releaseDate", F.to_timestamp("releaseDate"))\
            .withColumn("latestReleaseDate", F.to_timestamp("latestReleaseDate"))\
            .withColumn("eol", F.to_timestamp("eol"))
        
        return eol_ds
    
    def _read_brazilian_rf(self, path):
        @F.udf
        def gen_secao(x):
            x = int(x)
            if (x <= 3): return	"A - AGRICULTURA, PECUÁRIA, PRODUÇÃO FLORESTAL, PESCA E AQÜICULTURA"
            elif ( 5 <= x <= 9): return 	"B - INDÚSTRIAS EXTRATIVAS"
            elif ( 10 <= x <= 33): return	"C - INDÚSTRIAS DE TRANSFORMAÇÃO"
            elif ( 35 <= x <= 35): return	"D - ELETRICIDADE E GÁS"
            elif ( 36 <= x <= 39): return	"E - ÁGUA, ESGOTO, ATIVIDADES DE GESTÃO DE RESÍDUOS E DESCONTAMINAÇÃO"
            elif ( 41 <= x <= 43): return	"F - CONSTRUÇÃO"
            elif ( 45 <= x <= 47): return	"G - COMÉRCIO; REPARAÇÃO DE VEÍCULOS AUTOMOTORES E MOTOCICLETAS"
            elif ( 49 <= x <= 53): return	"H - TRANSPORTE, ARMAZENAGEM E CORREIO"
            elif ( 55 <= x <= 56): return	"I - ALOJAMENTO E ALIMENTAÇÃO"
            elif ( 58 <= x <= 63): return	"J - INFORMAÇÃO E COMUNICAÇÃO"
            elif ( 64 <= x <= 66): return	"K - ATIVIDADES FINANCEIRAS, DE SEGUROS E SERVIÇOS RELACIONADOS"
            elif ( 68 <= x <= 68): return	"L - ATIVIDADES IMOBILIÁRIAS"
            elif ( 69 <= x <= 75): return	"M - ATIVIDADES PROFISSIONAIS, CIENTÍFICAS E TÉCNICAS"
            elif ( 77 <= x <= 82): return	"N - ATIVIDADES ADMINISTRATIVAS E SERVIÇOS COMPLEMENTARES"
            elif ( 84 <= x <= 84): return	"O - ADMINISTRAÇÃO PÚBLICA, DEFESA E SEGURIDADE SOCIAL"
            elif ( 85 <= x <= 85): return	"P - EDUCAÇÃO"
            elif ( 86 <= x <= 88): return	"Q - SAÚDE HUMANA E SERVIÇOS SOCIAIS"
            elif ( 90 <= x <= 93): return	"R - ARTES, CULTURA, ESPORTE E RECREAÇÃO"
            elif ( 94 <= x <= 96): return	"S - OUTRAS ATIVIDADES DE SERVIÇOS"
            elif ( 97 <= x <= 97): return	"T - SERVIÇOS DOMÉSTICOS"
            elif ( 99 <= x <= 99): return	"U - ORGANISMOS INTERNACIONAIS E OUTRAS INSTITUIÇÕES EXTRATERRITORIAIS"
            return "DESCONHECIDA"
        
        rfb = self.spark_session.read.format("delta").load(path)\
            .withColumn("cnae_principal_raiz", F.substring(F.col("cnae_fiscal_principal_cod"), 0, 2))\
            .withColumn("cnae_secao", gen_secao(F.col("cnae_principal_raiz")))
        return rfb

    def _read_rdap_dataset(self, path):

        rdap = self.spark_session.read.parquet(path, compression="gzip")
        return rdap

    def _read_epss_dataset(self, path, check_update=False):

        if check_update:
            crawler = crawlers.FirstEPSS()
            crawler.download()

        epss = self.spark_session.read.format("delta").load(path)
        return epss

    def _read_rir_dataset(self, path, check_update=False):
        if check_update:
            crawler = crawlers.LACNICStatistics()
            crawler.download()

        epss = self.spark_session.read.format("delta").load(path)
        return epss
        

