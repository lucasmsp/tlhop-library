External Datasets & Crawlers
*****************************


Introduction
============

| We frequent use different external databases to enhance our analysis. To facilitate the use of multiple datasets, we implement Crawlers and Dataset API to manage all different sources.

| The crawler API is intended to assist in downloading external data sets, check for new versions, and in some cases, converting the original format to a better data schema, automating the entire process. Complementary to that, the Dataset API is intended to assist in manipulate the Dataset API is intended to help access each downloaded dataset, in a simple way.

| All crawlers are available at **tlhop.crawlers** (Futher information available in API Reference). Crawlers have two main methods, **describe** and **download**. The first method is responsible to generate a summary about the dataset, its source link and a small description. The second method is to download and register it. 

| When starting a Crawler, users must inform a path to use as datasets folder. This folder will contain all downloaded datasets (which one in its respective subfolder). If the informed path already has a subfolder containing the desired dataset, the Crawler will check if is available a new dataset version. After the usage of method **download**, the Crawler will create a file *RELEASE* inside its dataset folder. This file will have information to compare the current version with the available on Internet.

| Some Crawlers, as **NISTNVD** (responsible to download the National Vulnerability Database (NVD) from `NIST <https://www.nist.gov/>`), need to post-processing its data (for instance, adding a vulnerability rank based on CVE score). Because of its large amount of data, it is required, pass an active Spark Session as parameters. Besides that, all process is made transparently to user.

| Datasets created by us (all small datasets), that not contain an official link, are provided automatically when installing the **TLHOP library**. These datasets are called as `internal` while the others are referred as `external`. A current list of all supported datasets, and its respective Crawler name is available below.

+---------------------------+----------------------------------------------------------+-------------------+
| Code name                 | Description                                              | Crawler Name      |
+===========================+==========================================================+===================+
| HTTP_STATUS_FILE          | Hypertext Transfer Protocol (HTTP) Status Code Registry  | internal          |
+---------------------------+----------------------------------------------------------+-------------------+
| ISO_639_LANGUAGE          | Language code (iso 639) mapping list                     | internal          |
+---------------------------+----------------------------------------------------------+-------------------+
| ACCENTED_WORDS_PT_BR_FILE | List of accented pt-br words                             | internal          |
+---------------------------+----------------------------------------------------------+-------------------+
| PT_BR_DICTIONARY          | List of pt-br words with length more than 2 characteres  | internal          |
+---------------------------+----------------------------------------------------------+-------------------+
| UTF8_MAPPING_FILE         | UTF-8 Code mapping                                       | internal          |
+---------------------------+----------------------------------------------------------+-------------------+
| NVD_CVE_LIB               | NIST National Vulnerability Database                     | NISTNVD           |
+---------------------------+----------------------------------------------------------+-------------------+
| AS_RANK_FILE              | CAIDA's AS Rank                                          | ASRank            |
+---------------------------+----------------------------------------------------------+-------------------+
| AS_TYPE_FILE              | CAIDA's AS Classification                                | AS2Type           |
+---------------------------+----------------------------------------------------------+-------------------+
| BRAZILIAN_RF              | Brazilian National Register of Legal Entities - CNPJ     | BrazilianFR       |
+---------------------------+----------------------------------------------------------+-------------------+
| MIKROTIK_OS               | Mikrotik Operational System releases                     | MikrotikReleases  |
+---------------------------+----------------------------------------------------------+-------------------+
| BRAZILIAN_CITIES          | Brazil's cities information dataset                      | BrazilianCities   |
+---------------------------+----------------------------------------------------------+-------------------+
| CISA_EXPLOITS             | CISA's Known Exploited Vulnerabilities Catalog           | CISAKnownExploits |
+---------------------------+----------------------------------------------------------+-------------------+
| ENDOFLIFE                 | Keep track of lifecycles for various products            | EndOfLife         |  
+---------------------------+----------------------------------------------------------+-------------------+
| FIRST_EPSS                | FIRST's Exploit Prediction Scoring system (EPSS)         | FirstEPSS         | 
+---------------------------+----------------------------------------------------------+-------------------+
| LACNIC_STATISTICS         | LACNIC RIR Statistics                                    | LACNICStatistics  | 
+---------------------------+----------------------------------------------------------+-------------------+

Example of use: Download NVD/NIST Dataset and loading it
=========================================================

In this example, the method *download* (line 17) will download the original NIST files. If you already have a previous version, only new files will download (or with a different hash). After that, the crawler generates a new consolidated file (line 39), in parquet (more efficient format to handle this schema).

.. code-block:: python
   :linenos:

   >>> from tlhop.crawlers import NISTNVD

   >>> spark = SparkSession.builder.master("local[10]").getOrCreate()
   >>> nvd_crawler = NISTNVD()

   >>> nvd_crawler.describe()
      NIST NVD:
      - Description: The National Vulnerability Database (NVD) is the U.S. government 
        repository of standards based vulnerability management data.
      - Reference: https://nvd.nist.gov/
      - Download link: https://nvd.nist.gov/feeds/json/cve/1.1/nvdcve-1.1-{year}.json.zip
      - Fields: cve_id, description, cvssv2, cvssv3, publishedDate, lastModifiedDate, 
        baseMetricV2, baseMetricV3, cpe, references, published_year, rank_cvss_v2,
        rank_cvss_v3

   >>> nvd_crawler.download()
      [INFO] Downloading new file: 'nvdcve-1.1-2002.json.zip'
      [INFO] Downloading new file: 'nvdcve-1.1-2003.json.zip'
      [INFO] Downloading new file: 'nvdcve-1.1-2004.json.zip'
      [INFO] Downloading new file: 'nvdcve-1.1-2005.json.zip'
      [INFO] Downloading new file: 'nvdcve-1.1-2006.json.zip'
      [INFO] Downloading new file: 'nvdcve-1.1-2007.json.zip'
      [INFO] Downloading new file: 'nvdcve-1.1-2008.json.zip'
      [INFO] Downloading new file: 'nvdcve-1.1-2009.json.zip'
      [INFO] Downloading new file: 'nvdcve-1.1-2010.json.zip'
      [INFO] Downloading new file: 'nvdcve-1.1-2011.json.zip'
      [INFO] Downloading new file: 'nvdcve-1.1-2012.json.zip'
      [INFO] Downloading new file: 'nvdcve-1.1-2013.json.zip'
      [INFO] Downloading new file: 'nvdcve-1.1-2014.json.zip'
      [INFO] Downloading new file: 'nvdcve-1.1-2015.json.zip'
      [INFO] Downloading new file: 'nvdcve-1.1-2016.json.zip'
      [INFO] Downloading new file: 'nvdcve-1.1-2017.json.zip'
      [INFO] Downloading new file: 'nvdcve-1.1-2018.json.zip'
      [INFO] Downloading new file: 'nvdcve-1.1-2019.json.zip'
      [INFO] Downloading new file: 'nvdcve-1.1-2020.json.zip'
      [INFO] Downloading new file: 'nvdcve-1.1-2021.json.zip'
      [INFO] Downloading new file: 'nvdcve-1.1-2022.json.zip'
      [INFO] Generating new consolidated file.
      [INFO] New dataset version is download with success!


After use an crawler, it required to update the list of available datasets by running the code:

.. code-block:: python
   :linenos:

   >>> from tlhop.datasets import DataSets

   >>> ds = DataSets()
   >>> ds.list_datasets() # generate a list of all currently available datasets and their codes identification.
   >>> nvd = ds.read_dataset("NVD_CVE_LIB") # read NVD dataset as a Spark DataFrame.

------------


**Obs:** Further examples can be found in the `examples <https://github.com/lucasmsp/tlhop-library/tree/main/examples/>`_ directory. 

   
