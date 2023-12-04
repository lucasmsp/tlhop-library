import pyspark.sql.functions as F 
import xml.etree.ElementTree as ET

import os
import glob

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import *  # needed to be easy to create fingerprints in xml

class Fingerprints(object):

    _ERROR_MESSAGE_000 = "[ERROR] This algorithm requires an environment variable 'TLHOP_DATASETS_PATH' containing a folder path to be used as storage to all TLHOP datasets."
    _ERROR_MESSAGE_001 = "[ERROR] None active Spark session was found. Please start a new Spark session before use DataSets API."
    _ERROR_MESSAGE_002 = "[ERROR] This algorithm requires an environment variable 'TLHOP_FINGERPRINTS_PATH' containing a folder with fingerprints in a xml."
    _ERROR_MESSAGE_003 = "[ERROR] None fingerprints were found"

    def __init__(self, custom_path=None):

        root_path = os.environ.get("TLHOP_DATASETS_PATH", None)
        if not root_path:
            raise Exception(self._ERROR_MESSAGE_000)
        self.root_path = (root_path+"/").replace("//", "/")

        self.spark_session = SparkSession.getActiveSession()
        if not self.spark_session:
            raise Exception(self._ERROR_MESSAGE_001)
        
        self.fingerprints = {}
        self.output = None
        self.output_col = "meta_events"

        self.basepath = self.root_path + "fingerprints"
        self.output_path = self.basepath + "/fingerprints.delta"
        
        if custom_path:
            fingerprints_path = custom_path
        else:
            fingerprints_path = os.environ.get("TLHOP_FINGERPRINTS_PATH", None)
            if not fingerprints_path:
                raise Exception(self._ERROR_MESSAGE_002)
            fingerprints_path += "/*.xml"
        
        for input_file in glob.glob(fingerprints_path):
            self._populate_fingerprint_dict(input_file)

        if len(self.fingerprints) == 0:
            raise Exception(self._ERROR_MESSAGE_003)

        if not os.path.exists(self.basepath):
            os.mkdir(self.basepath)


    def run(self, input_df, output_col="meta_events"):

        _ = self._find_all_fingerprints(input_df, output_col)
        response = _persist_fingerprints_mapping()
        return response

        
    def _find_all_fingerprints(self, input_df, output_col="meta_events"):
        """
        Method to analize all records and columns to map some specificic events of a record in a new `meta_events` column.
        The mapped events can be like: it has screenshots; it has some compromised database or has other type of vulnerability;
        and others.
    
        :return: Spark DataFrame 
        """
        
        tmp_df = input_df
        new_cols = []
        self.output_col = output_col
        
        for new_col, cond in self.fingerprints.items():
            tmp_col = "tmp-" + new_col
            new_cols.append(tmp_col)
            tmp_df = tmp_df.withColumn(tmp_col, F.when(cond, F.lit(new_col)))
        
        self.output = tmp_df.withColumn(output_col, F.array_sort(F.array_distinct(F.split(F.concat_ws(",", *new_cols), ","))))\
            .withColumn(output_col, F.expr('filter(' + output_col + ', (x -> length(x) > 0))'))\
            .drop(*new_cols)
    
        return self.output

    def _persist_fingerprints_mapping(self):

        self.output.filter(size("meta_events") > 0)\
          .select("year", "meta_module", "date", "shodan_id", self.output_col)\
          .repartition(1, ["year", "date", "meta_module"])\
          .write\
          .parquet\
          .mode("overwrite")\
          .partitionBy("year", "date", "meta_module")\
          .save(self.output_path)
        
        return True

    def _populate_fingerprint_dict(self, input_file):
        """

        """
        tree =  ET.parse(input_file)

        for item in tree.getroot().findall("fingerprint"):
            label = item.find("label").text
            condition = item.find("condition").text
            if label in self.fingerprints:
                print(f"Label {label} is duplicated")
            self.fingerprints[label] = eval(condition)

        return 



        