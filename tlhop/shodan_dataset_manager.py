#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import json
import os
import glob
from itertools import chain
from datetime import datetime
import sys

from sklearn.neighbors import KNeighborsClassifier
from joblib import dump, load
import pickle
from pathlib import Path

from pyspark.sql.types import *
import pyspark.sql.functions as F

from delta.tables import DeltaTable
from delta.tables import *

from tlhop.shodan_abstraction import DataFrame
from tlhop.datasets import DataSets
from tlhop.library import *


class ShodanDatasetManager(object):
    
    
    
    _ERROR_MESSAGE_001 = "None active Spark session was found. Please start a new Spark session before use the API."
    
    def __init__(self, output_folder, output_log=None):
        """
        A Class to convert Shodan's Json files into a single Delta format, to enrich and manage the dataset.
        
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
        
    def convert_files(self, input_list_files, org_refinement=True, fix_brazilian_cities=True):
        """
        Converts all JSON files in `input_list_files` into a single DELTA file format.
        
        :params input_list_files: A list of input filepaths;
        :params org_refinement: True to add a new column (org_clean) containing the 
         name of the organization in a standardized version;  
        :params fix_brazilian_cities: If true, it fixes brazilian cities and region codes names (default, True).   
        """
        
        if fix_brazilian_cities:
            self._load_KNNClassifier_to_brazilian_cities()     
        times = []
        
        for filepath in input_list_files:
            filename = os.path.basename(filepath)
            self._logger("[INFO] Reading {} and saving as {}".format(filename, self.OUTPUT_FOLDER))

            t1 = time.time()
            self._convert(filepath, self.OUTPUT_FOLDER, org_refinement, fix_brazilian_cities)
            t2 = time.time()
            elapsed_time = int(t2-t1)
            times.append(elapsed_time)
            self._logger("[INFO] Total time is {} seconds.".format(elapsed_time))
        
        n = len(times)
        total_time = sum(times)
        avg_time = total_time/len(times)
        self._logger(f"[INFO] All {n} files converted in {total_time} seconds, an average of {avg_time:.2f} seconds per file.")
    
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

#     def infering_missing_organization_name(self, df):
#         """
        
#         """
        
#         raise Exception("Sorry, not implemented yet!")
#         # Preencher organização ausente baseado em IP, e para os que possui o campo, gere a versão padronizada
#         # aqui, só consideramos o dia
# #         part_not_ok = df.filter(col("org").isNull() & col("ip_str").isNotNull())\
# #             .shodan_extension.get_ip_mask(input_col="ip_str", output_col="mask3", level=3)\
# #             .join(mask3.hint("broadcast"), on=["mask3"])\
# #             .drop("mask3")

# #         part_ok = df.filter(col("org").isNotNull() | col("ip_str").isNull())\
# #             .shodan_abstraction.cleaning_org(input_col="org", output_col="org_clean")

# #         df = part_ok.union(part_not_ok)
#         return df

    def _logger(self, msg):
        """
        A dummy method for recording events on screen and ensuring their persistence in a file.
        """
        print(msg, flush=True)

        if self.LOG_OUTPUT:
            with open(self.LOG_OUTPUT, "a") as f:
                f.write(msg+"\n")    
            
    def _get_tlhop_auxiliar_folder(self):
        root_path = os.environ.get("TLHOP_DATASETS_PATH", None)
        if not root_path:
            raise Exception("In order to fix city and region_code columns you must set the TLHOP_DATASETS_PATH")

        path = root_path + "/auxiliar"
        if not os.path.exists(path):
            os.makedirs(path)
        return path
    
    def _load_KNNClassifier_to_brazilian_cities(self):
        """
        This method uses the BRAZILIAN_CITIES dataset (available in the 
        THLOP's Crawlers/DataSets API) to generate a model,
        based on K-NN to predict the city/region_code based
        on latitude and longitude.
        """

        path = self._get_tlhop_auxiliar_folder()
            
        LocationModelPath = path + '/tlhop_cities_model.pickle'
        
        if os.path.exists(LocationModelPath):
            locationModel = load(LocationModelPath)
        else:
            self._logger("City/Region classifier not found. Creating a new model...")
            ds = DataSets()

            brazil_cities = ds.read_dataset("BRAZILIAN_CITIES")
            br_cities = brazil_cities.select("CITY", "STATE", "LONG", "LAT").toPandas()
            brazil_cities.unpersist()
            cities_orders = br_cities["CITY"].tolist()
            states_orders = br_cities["STATE"].tolist()
            X = br_cities[["LAT", "LONG"]].to_numpy()

            # A lat/lon são chaves para a localidade. Existem registros com lat/long com cidades 
            #e region_codes, além disso, existem aqueles com cidade mas não de region_code

            neigh = KNeighborsClassifier(n_neighbors=1)
            y = [i for i in range(len(X))]
            neigh.fit(X, y)
            locationModel = {
                "model": neigh,
                "cities_orders": cities_orders, 
                "states_orders": states_orders
            }
            dump(locationModel, LocationModelPath) 
            self._logger("Model Created!")

        self.locationModel = locationModel
        self._logger("City/Region classifier model loaded !")

    
    def _gen_initial_schema(self, df):
        """
        Converts a StructType() (dictionaries/sub-jsons) to json-string, 
        except  "location" and "_shodan" columns (both columns will have 
        their subcolumns moved to root). Also converts ArrayType(StructType())
        to json-string.
        """
        schema = df.schema
        path = self._get_tlhop_auxiliar_folder()
        self._update_persisted_schema(path, schema)
        
        for field in df.schema:
            name = field.name
            dtype = field.dataType

            if isinstance(dtype, StructType):
                if (name not in ["location", "_shodan"]):
                    schema[name].dataType = StringType()

            elif isinstance(dtype, ArrayType):
                if isinstance(schema[name].dataType.elementType, StructType):
                    schema[name].dataType = StringType()
                    
        return schema
    
    def _update_persisted_schema(self, root_path, schema):
        """
        Persists the original Shodan schema.
        """
        path = root_path + "/original_schema.pickle"
        if os.path.exists(path):
            with open(path, 'rb') as f:
                initial_schema = pickle.load(f)
        else:
            initial_schema = {}
            
        for field in schema:
            name = field.name
            initial_schema[name] = json.dumps(field.jsonValue())

        with open(path, 'wb') as f:
            pickle.dump(initial_schema, f)
        

    def _extracting_complex_json_columns(self, df):
        """
        The column `location` (frequent) is a composite attribute with 10 
        fixed fields such as `latitude`, `longitude`, among others. 
        We moved these subcolumns to the first level as simple attributes.
        
        We also moved the `shodan` subcolumns to the first level, but in this case,
        we added the `shodan_` prefix in their name.
        """
        
        df = df.select(_flatten_struct(df.schema, ['location']))
        # each _flatten_struct must be separated by an instantiation to update the schema layout.
        df = df.select(_flatten_struct(df.schema, ['_shodan'], "shodan_"))\
                .withColumn("shodan_ptr", F.when(F.col("shodan_ptr") == "true", True).otherwise(False))\
                .withColumn("shodan_options", F.to_json("shodan_options"))
        return df
    
    def _cleaning_empty_columns(self, df):
        """
        Convert empty arrays and empty jsons (`{}`) to NULL. It also converts 
        some values (that represents empty informations) to NULL, such as:
        
        - Column `coap`, value: `{"resources":{}}` 
        - Column `iscsi`, value: `{"targets":[]}`
        - Column `data`, value: `""`
        - Column `bgp`, value: `{"error_code":"Cease","error_subcode":"Connection Rejected","length":"21","type":"NOTIFICATION"}`
        """

        for name, dtype in df.dtypes:
            if "array" in dtype:
                df = df.withColumn(name, F.when((F.size(F.col(name)) == 0), F.lit(None)).otherwise(F.col(name)))
                
        df = df.replace("{}", None)\
            .replace("", None, subset=['data'])\
            .replace('{"resources":{}}', None, subset=['coap'])\
            .replace('{"targets":[]}', None, subset=['iscsi'])\
            .replace('{"error_code":"Cease","error_subcode":"Connection Rejected","length":"21","type":"NOTIFICATION"}', None, subset=['bgp'])
                
        return df
    
    def _force_casting(self, df):
        """
        Method to force the right datatype for columns: 
        timestamp, ip, port, hash, latitude and longitude;
        """
        df = df.withColumn("timestamp", F.col("timestamp").cast("timestamp"))\
            .withColumn("ip", F.col("ip").cast("long"))\
            .withColumn("port", F.col("port").cast("int"))\
            .withColumn("hash", F.col("hash").cast("long"))\
            .withColumn("latitude", F.col("latitude").cast("double"))\
            .withColumn("longitude", F.col("longitude").cast("double"))
        
        return df
    
    
    def _fixing_http_html_columns(self, df):
        """
        Removal of `title` and `html` columns due to obsolescence. Information is now present in `http`.
        """
        df = df.withColumn("http", F.when((F.col("html").isNotNull() |  F.col("title").isNotNull()) & F.col("http").isNull(), 
                                         F.to_json(F.struct("html", "title"))
                                        ).otherwise(F.col("http")))\
            .drop("html", "title")
        
        return df
    
    
    def _fixing_location(self, df, fix_brazilian_cities):
        
        """
        Some records have location information wrong. In some cases,
        region_code is a number, other cases is a string. City or region 
        can also be missing. This method uses your model 
        (_load_KNNClassifier_to_brazilian_cities) to verified these 
        informations based on lat/long information. `city` column is 
        also converted to capital letters.
        """
        
        if fix_brazilian_cities:
            df = df.withColumn("fixing_location", 
                               F.when((~_is_valid_uf_code(F.col("region_code"))) &
                                      (F.col("latitude").isNotNull()) & 
                                      (F.col("longitude").isNotNull()), 
                                _find_real_location_udf(self.locationModel)(F.col("latitude"), F.col("longitude"))
                               ).otherwise(F.struct(*[F.col('city').alias('city'), 
                                                      F.col("region_code").alias('region_code')])))\
                    .drop("city","region_code")\
                    .select("*", "fixing_location.*")\
                    .drop("fixing_location")
        
        df = df.withColumn("city", F.upper("city"))
        
        return df
        
    
    def _changing_vulns_layout(self, df):
        """
        Changing layout of `vulns` column. The new layout will be composed of two columns:
        `vulns_cve` (array<string>) containing all cve codes; `vulns_verified` 
        (array<string>) containing all Shodan's verified cve codes. 
        """
        
        df = df.withColumn("tmp", _convert_vulns_udf("vulns"))\
                .select("*", "tmp.*")\
                .drop("tmp", "vulns")
        
        return df
    
    def _convert(self, input_filepath, output_filepath, org_refinement, fix_brazilian_cities):
        """
        
        - Convert multiples `json` files to a single `delta` file;
        - Fix: `city` and `region_code` columns have their information verified based on lat/long information. `city` is also converted to capital letters;
        - New `org_clean` column from the `cleaning_org()` method, with all sorting processing except using the conversion table;
        - Removal of columns `dma_code`, `country_code3`, `postal_code`, `area_code` because this information is not available in Brazil;
        - New `meta_module` column, where values are module dependent : "https", "http", "http-simple-new", "https-simple-new", "auto", "rdp", otherwise "mixed_modules";
        - Removal of `cpe` column: The cpe column referring to CPEs 
          detected in the banner has already been marked as obsolete. 
          The usage of the new cpe23 column is recommended. Considering data 
          from 2021, in all data that the cpe column was present, the new column 
          was also present;
        - Removal of `title` and `html` columns due to obsolescence. Information is 
          now present in `http`;
        - We use `gzip` as the compression method in parquet. The other `snappy` option, although is faster for reading, has less compression;  
        
        """
        try:
            # infers all primitive values as a string type
            df = self.spark_session.read.json(input_filepath, primitivesAsString=True)
        except:
            self._logger("[ERROR] Memory error while Spark tried to infer the JSON schema."\
                  "Try to increase 'spark.sql.files.minPartitionNum' to more than 3 "\
                  "x n_cores in your Spark Session, after that, run `convert_files()` again.")
            print(traceback.format_exc())
            
        self._logger(f"[INFO] Schema inferred to all columns. Starting conversion.")
        
        schema = self._gen_initial_schema(df)
        df = self.spark_session.read.json(input_filepath, schema=schema)
        df = self._extracting_complex_json_columns(df)
        df = self._force_casting(df)
        df = self._fixing_http_html_columns(df)
        df = self._fixing_location(df, fix_brazilian_cities)
        df = self._changing_vulns_layout(df)
        df = self._cleaning_empty_columns(df)
           
        drop_unwanted_columns = ["dma_code", "country_code3", "postal_code", "area_code", 'cpe']
        
        df = df.withColumn("date", F.to_date("timestamp"))\
                .withColumn("year", F.year("timestamp"))\
                .withColumn("meta_module", _get_tag_udf(F.col("shodan_module")))\
            .drop(*drop_unwanted_columns)
        
        if org_refinement:
            df = df.shodan_extension.cleaning_org(input_col="org", output_col="org_clean")

        # Logging e salvando o resultado
        columns = df.columns
        self._logger("[INFO] {} columns: {}".format(len(columns), columns))  

        df.write\
          .format("delta")\
          .mode("append")\
          .option("mergeSchema", "true")\
          .partitionBy("year", "date", "meta_module")\
          .save(output_filepath)      


def _find_real_location(lat, lon, model):
    if lat and lon:
        i = model["model"] .predict([[lat, lon]]).tolist()[0]
        return model["cities_orders"][i], model["states_orders"][i]
    else:
        return "ERROR", "ERROR"        

schema_address = StructType()\
    .add("city", StringType(), True)\
    .add("region_code", StringType(), True, None)

def _find_real_location_udf(model):
    return F.udf(lambda lat, long: _find_real_location(lat, long, model), schema_address)

@F.udf
def _get_tag_udf(shodan_module):
    """
    Create a new column based on shodan_module value. 
    """
    if shodan_module in ["https", "http", "http-simple-new", "https-simple-new", "auto", "rdp"]:
        return shodan_module
    else:
        return "mixed-modules"        
        
schema1 = StructType()\
    .add("vulns_cve", ArrayType(StringType()), True)\
    .add("vulns_verified", ArrayType(StringType()), True, None)

@F.udf(returnType=schema1)
def _convert_vulns_udf(row):
    """
    Method to transform `vulns` column (in json) into two new columns:
    a list of CVEs and a list of verified CVEs.
    """

    if row:
        row = json.loads(row)
        cves = list(row.keys())
        verifieds = []
        
        for r in row:
            if row[r].get("verified", "false") == "true":
                verifieds.append(r)
                
        if len(verifieds) == 0:
            verifieds = None
            
        results = [cves, verifieds]
    else:
        results = [None, None]
    return results


@F.udf(returnType=BooleanType())
def _is_valid_uf_code(code):
    """
    Check if a Shodan Region Code is valid in Brazil.
    """
    uf_codes = {
        'AC': 'Acre', 'AL': 'Alagoas', 'AM': 'Amazonas', 'AP': 'Amapá', 
        'BA': 'Bahia', 'CE': 'Ceará', 'DF': 'Distrito Federal', 
        'ES': 'Espírito Santo', 'GO': 'Goiás', 'MA': 'Maranhão', 
        'MT': 'Mato Grosso', 'MS': 'Mato Grosso do Sul', 
        'MG': 'Minas Gerais', 'PA': 'Pará', 'PB': 'Paraíba', 
        'PR': 'Paraná', 'PE': 'Pernambuco', 'PI': 'Piauí', 
        'RJ': 'Rio de Janeiro', 'RN': 'Rio Grande do Norte', 
        'RS': 'Rio Grande do Sul', 'RO': 'Rondônia', 
        'RR': 'Roraima', 'SC': 'Santa Catarina', 'SP': 'São Paulo', 
        'SE': 'Sergipe', 'TO': 'Tocantins'
    }
    
    if code:
        return code in uf_codes
    else:
         return False
        
def _flatten_struct(schema, cols, prefix=""):
    result = []
    for elem in schema:
        if elem.name not in cols:
            result.append(F.col(elem.name))
        elif isinstance(elem.dataType, StructType):
            for subcol in elem.dataType :
                result.append(F.col(elem.name+"."+subcol.name).alias(prefix+subcol.name))
        else:
            print("{} isn't a structType()".format(elem.name))
    return result