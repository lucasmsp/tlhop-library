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

import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

from tlhop.library import cleaning_text_udf


class BrazilianFR(object):
    
    """
    Brazilian National Register of Legal Entities - CNPJ

    The National Register of Legal Entities (CNPJ) is a database managed by the Special 
    Secretariat of the Federal Revenue of Brazil (RFB), which stores registration information of legal 
    entities and other entities of interest to the tax administrations of the Union, States, District 
    Federal and Municipalities.
    
    Reference: https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica-cnpj
    """
    
    
    _INFO_MESSAGE_001 = "[INFO] Last crawling timestamp: {}"
    _INFO_MESSAGE_002 = "[INFO] A most recent version of the current dataset was found."
    _INFO_MESSAGE_003 = "[INFO] The current dataset version is the most recent."
    _INFO_MESSAGE_004 = "[INFO] New dataset version is download with success!"
    _INFO_MESSAGE_005 = "[INFO] Downloading new file: '{}'"
    _INFO_MESSAGE_006 = "[INFO] Starting to process files."
    _INFO_MESSAGE_007 = "[INFO] Download Aborted"
    _INFO_MESSAGE_008 = "[INFO] Processing '{}'"
    _INFO_MESSAGE_009 = "[INFO] Processed '{}' - number of records: {}"
    _INFO_MESSAGE_010 = "[INFO] Writing a consolidated file"

    _WARN_MESSAGE_001 = "[WARN] The download of all required files can take some time. "\
                  "If you want, you can abort this operation, download all zip files manually, "\
                  "place them inside `{}` folder and them resume the operation using the parameter `manual=True` "\
                  "to complete the setup.\nRequired files to download: {}\nDo you want to continue ? "\
                  "Press [Y/N] to continue or abort."
    _WARN_MESSAGE_002 = "[WARN] Do you want to remove all download zipped files? [Y/N]"
    
    _ERROR_MESSAGE_000 = "[ERROR] This crawler requires an environment variable 'TLHOP_DATASETS_PATH' containing a folder path to be used as storage to all TLHOP datasets."
    _ERROR_MESSAGE_001 = "[ERROR] Mentioned file '{}' in RELEASE was not found."
    _ERROR_MESSAGE_002 = ("[ERROR] Destination output already exists ('{}') but RELEASE file is "\
        "missing. Because of that, the crawler will not take in count previous files, if they exists.")
    _ERROR_MESSAGE_004 = "None active Spark session was found. Please start a new Spark session before use DataSets API."
    
    
    def __init__(self):
        """
        During the initialization process, it will check if a TLHOP directory folder is set and valid.
        It also will check on the Internet if an new dataset version is available to download in case 
        of a previous download was found during folder lookup. Because the download data will be 
        pos-processed to an efficient format, the initialization process will also check if a Spark 
        session is already created.
        """

        self.download_url = "http://200.152.38.155/CNPJ/"
        self.path = "brazilian-rf/"
        self.expected_schema = {
            "outname": "brazilian-rf-consolidated.gz.delta", 
            "files": ["Cnaes", "Empresas", "Estabelecimentos", 
                      "Naturezas", "Paises", "Municipios", "Motivos"] 
        }
        self.new_version = None
        self.last_file = {}
        self.now = datetime.now()
        self.year = self.now.year

        self.spark_session = SparkSession.getActiveSession()
        if not self.spark_session:
            raise Exception(self._ERROR_MESSAGE_004)
        # To Spark be able to write dates prior to 1900 in Parquet
        self.spark_session.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        self.spark_session.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        
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
                print(self._ERROR_MESSAGE_001.format(self.expected_schema["outname"]))
            
            with open(self.basepath+"RELEASE", "r") as f:
                for line in f.read().split("\n"):
                    if len(line) > 1:
                        url, etag, timestamp, raw_version = line.split("|")
                        filename = url.split("/")[-1]
                        if not os.path.exists(self.basepath + raw_version + "/" + filename):
                            raise Exception(self._ERROR_MESSAGE_002.format(filename))

                        self.last_file[url] = {"etag": etag, "timestamp": timestamp}
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
                        if any([ True 
                                for x in self.expected_schema["files"] 
                                if x in node.get('href')])
                       ])
                
        found_update = False
        for url in files:
            info = urllib.request.urlopen(url)
            etag = info.info()["ETag"]
            
            if url not in self.last_file:
                self.last_file[url] = {"etag": None}
                
            if etag != self.last_file[url]["etag"]:
                found_update = True
                self.last_file[url]["download"] = True
                self.last_file[url]["etag"] = etag
                self.last_file[url]["timestamp"] = self.now
            else:
                self.last_file[url]["download"] = False
        
        if found_update:
            print(self._INFO_MESSAGE_002)
        else:
            print(self._INFO_MESSAGE_003)
    
            
    def describe(self):
        """
        Function to describe some informations about the dataset: 
        description, reference and fields.
        """
        print("""
        # Brazilian National Register of Legal Entities - CNPJ
        
        - Description: The National Register of Legal Entities (CNPJ) is a database managed by the Special 
          Secretariat of the Federal Revenue of Brazil (RFB), which stores registration information of legal 
          entities and other entities of interest to the tax administrations of the Union, States, District 
          Federal and Municipalities.
        - Reference: https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica-cnpj
        - Download link: {}
        - Datasets: 
            - Company: company registration data at matrix level
            - Establishment: company analytical data by unit / establishment (phones, address, branch, etc.)
            - Cnae: code and description of CNAEs
            - Legal nature: table of legal natures - code and description.
            - Motivation: table of reasons for the cadastral situation - code and description.
            - Country: country table - code and description.
            - Municipality: table of municipalities - code and description.
        """.format(self.download_url))
    
    def download(self, manual=False):
        """
        Downloads a new dataset version available in the source link. After download, it process the
        original data format to enrich its data and to convert to Parquet format.

        :params manual: Because the download can take some time, users can download manually and place them at 
            `<datasets_path>/brazilian-rf/raw/`. To inform that users already download manually, use parameter 
            `manual = True`. Otherwise, the API will handle the download, one by one, automatically. 
            In both cases, after this step, the API will process these files by Spark to create a consolidated version.
        """
        raw_directory = self.basepath + "raw-" + datetime.today().strftime("%Y%m")
        
        if not manual:
            op = ""
            while op not in ["Y", "N"]:
                op = input(self._WARN_MESSAGE_001.format(raw_directory, self.last_file.keys())).upper()
            
            if op == "N":
                print(self._INFO_MESSAGE_007)
                return 

            if not os.path.exists(raw_directory):
                os.mkdir(raw_directory)
            
            for url in self.last_file:
                filename = url.split("/")[-1] 
                filepath = raw_directory +"/"+ filename

                if self.last_file[url]["download"]:
                    print(self._INFO_MESSAGE_005.format(filename))
                    fin = requests.get(url, allow_redirects=True)
                    fout = open(filepath, 'wb')
                    fout.write(fin.content)
                    fout.close()
                    fin.close()
        
        for url in self.last_file:
            filename = url.split("/")[-1] 
            filepath = raw_directory +"/"+ filename
            with zipfile.ZipFile(filepath, 'r') as zip_ref:
                # zip_ref.infolist()[0].date_time
                zip_ref.extractall(raw_directory+"/")

        outfile = self.basepath + self.expected_schema["outname"]
        self._process(raw_directory+"/", outfile)
        
        to_remove = glob.glob(raw_directory+"/*CSV")
        to_remove += glob.glob(raw_directory+"/*ESTABELE")
        for f in to_remove:
            os.remove(f)
        
        op = ""
        while op not in ["Y", "N"]:
            op = input(self._WARN_MESSAGE_002).upper()
                
        if op == "Y":
            for f in glob.glob(raw_directory+"/*.zip"):
                os.remove(f)
        
        now = self.now.strftime("%Y%m%d_%H%M%S")
        msg = ""
        for key, values in self.last_file.items():
            msg += "{}|{}|{}|{}\n".format(key, values["etag"], now, raw_directory)
        msg = msg[0:-1]
        f = open(self.basepath+"RELEASE", "w")
        f.write(msg)
        f.close()
        print(self._INFO_MESSAGE_004)
        return True

    def _process(self, input_folder, outfile):
        """
        Read all extracted files and creates a consolidated version, merging all files.
        """
        print(self._INFO_MESSAGE_006)
        
        
        
        INPUT_ESTALECIMENTOS = f"{input_folder}*ESTABELE*"
        INPUT_PAISES = f"{input_folder}*PAISCSV"
        INPUT_MUNICIPIOS = f"{input_folder}**MUNICCSV" 
        INPUT_MOTIVOS = f"{input_folder}*MOTICSV" 
        INPUT_CNAES = f"{input_folder}*CNAECSV" 
        INPUT_EMPRESAS = f"{input_folder}*EMPRECSV" 
        INPUT_NATUREZA = f"{input_folder}*NATJUCSV"
        
        paises = self.spark_session.read.csv(INPUT_PAISES, sep= ";", encoding="iso-8859-1")\
            .withColumnRenamed("_c0", "cod_pais")\
            .withColumnRenamed("_c1", "pais")
        
        municipios = self.spark_session.read.csv(INPUT_MUNICIPIOS, sep= ";", encoding="iso-8859-1")\
            .withColumnRenamed("_c0", "codigo_mun")\
            .withColumnRenamed("_c1", "endereco_municipio")
        
        motivos = self.spark_session.read.csv(INPUT_MOTIVOS, sep= ";", encoding="iso-8859-1")\
            .withColumnRenamed("_c0", "cod_motivo")\
            .withColumnRenamed("_c1", "situacao_cadastral_motivo")
        
        cnaes = self.spark_session.read.csv(INPUT_CNAES, sep= ";", encoding="iso-8859-1")\
            .withColumnRenamed("_c0", "cod_cnea")\
            .withColumnRenamed("_c1", "cnae_descricao")
        
        naturezas = self.spark_session.read.csv(INPUT_NATUREZA, sep= ";", inferSchema=False, encoding="iso-8859-1")\
            .withColumnRenamed("_c0", "cod_natureza")\
            .withColumnRenamed("_c1", "natureza_juridica")
    
        print(self._INFO_MESSAGE_008.format("estabelecimentos"))
        
        estabelecimentos = self.spark_session.read.csv(INPUT_ESTALECIMENTOS, sep=";",
                    header=False,  encoding="iso-8859-1", ignoreLeadingWhiteSpace=True, 
                    ignoreTrailingWhiteSpace=True,  maxColumns=30, schema=schema_estabelecimentos,
                    escape="\"", charToEscapeQuoteEscaping="\\")\
            .drop("contato_ddd_fax")\
            .withColumn("nome_fantasia_clean", cleaning_text_udf(F.col("nome_fantasia"), F.lit(True)))\
            .withColumn("situacao_cadastral", F.when(F.col("situacao_cadastral") == 1, "NULA")\
                                .when(F.col("situacao_cadastral") == 2, "ATIVA")\
                                .when(F.col("situacao_cadastral") == 3, "SUSPENSA")\
                                .when(F.col("situacao_cadastral") == 4, "INAPTA")\
                                .when(F.col("situacao_cadastral") == 8, "BAIXADA")\
                                .otherwise("DESCONHECIDO"))\
            .withColumn("identificador_matriz", F.when(F.col("identificador_matriz") == 1, "MATRIZ")\
                                .when(F.col("identificador_matriz") == 2, "FILIAL")\
                                .otherwise("DESCONHECIDO"))\
            .withColumn("cnae_categorias", _categoria_cnae(F.col("cnae_fiscal_principal_cod"), 
                                                          F.col("cnae_fiscal_secundaria_cod")))\
            .withColumn("situacao_cadastral_data", 
                        F.to_date(F.col("situacao_cadastral_data"), "yyyyMMdd"))\
            .withColumn("data_inicio_atividade", 
                        F.to_date(F.col("data_inicio_atividade"), "yyyyMMdd"))\
            .withColumn("situacao_especial_data", 
                        F.to_date(F.col("situacao_especial_data"), "yyyyMMdd"))\
            .persist()
        
        n = estabelecimentos.count()
        print(self._INFO_MESSAGE_009.format("estabelecimentos", n))  
        print(self._INFO_MESSAGE_008.format("empresas"))
        
        empresas = self.spark_session.read.csv(INPUT_EMPRESAS, sep=";", encoding="iso-8859-1", maxColumns=7,
                                               schema=schema_empresas, escape="\"", charToEscapeQuoteEscaping="\\", 
                                               ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True)\
            .withColumn("porte_empresa", F.when(F.col("porte_empresa") == "00", "NAO INFORMADO")\
                                    .when(F.col("porte_empresa") == "01", "MICRO EMPRESA")\
                                    .when(F.col("porte_empresa") == "03", "EMPRESA DE PORTE PEQUENO")\
                                    .otherwise("DEMAIS"))\
            .withColumn("razao_social_clean", cleaning_text_udf(F.col("razao_social"), F.lit(True)))\
            .join(naturezas.hint("broadcast"), F.col("cod_natureza") == F.col("cod_natureza_juridica"), "left")\
            .drop("cod_natureza", "cod_natureza_juridica")\
            .persist()
        
        n = empresas.count()
        print(self._INFO_MESSAGE_009.format("empresas", n))
        print(self._INFO_MESSAGE_010)
        
        df_consolidado = estabelecimentos.join(paises.hint("broadcast"), F.col("codigo_pais") == F.col("cod_pais"), "left")\
            .drop(*["codigo_pais", "cod_pais", "contato_ddd2", "contato_telefone2", "contato_fax"])\
            .fillna("NAO DECLARADOS", subset=['pais'])\
            .join(municipios.hint("broadcast"), F.col("codigo_mun") == F.col("endereço_cod_municipio"), "left")\
            .drop(*["codigo_mun", "endereço_cod_municipio"])\
            .join(motivos.hint("broadcast"), F.col("cod_motivo") == F.col("situacao_cadastral_motivo_cod"), "left")\
            .drop(*["cod_motivo", "situacao_cadastral_motivo_cod"])\
            .join(cnaes.hint("broadcast"), F.col("cod_cnea") == F.col("cnae_fiscal_principal_cod"), "left")\
            .drop("cod_cnea")\
            .join(empresas, "cnpj_basico")\
            .withColumn("cnpj", F.concat_ws("", F.col("cnpj_basico"), F.col("cnpj_ordem"), F.col("cnpj_dv")))\
            .drop("cnpj_ordem", "cnpj_dv")\
            .withColumn("dump_data", lit(self.now.strftime("%Y-%m-%d")).cast("date"))
        
        if os.path.exists(outfile):
            deltaTable = DeltaTable.forPath(self.spark_session, outfile)
            deltaTable.alias("old").merge(
                df_consolidado.alias("new"),
                (col("old.cnpj") == col("new.cnpj")) & (col("old.razao_social") == col("new.razao_social")) & 
                (
                     (col("old.nome_fantasia").isNull() & col("new.nome_fantasia").isNull()) |
                     (col("old.nome_fantasia").isNotNull() & col("new.nome_fantasia").isNotNull() & (col("old.nome_fantasia") == col("new.nome_fantasia")))
                )
            )\
            .whenNotMatchedInsertAll()\
            .execute()

            deltaTable.optimize().executeCompaction()
            deltaTable.vacuum(0)
        else:
            df_consolidado.write.format("delta").save(outfile)

        estabelecimentos.unpersist()
        empresas.unpersist()



schema_empresas = StructType([
    StructField("cnpj_basico", StringType(), True),
    StructField("razao_social", StringType(), True),
    StructField("cod_natureza_juridica", StringType(), True),
    StructField("qualificacao_responsavel", StringType(), True),
    StructField("capital_social_empresa", StringType(), True),
    StructField("porte_empresa", StringType(), True),
    StructField("ente_responsavel", StringType(), True)
])


schema_estabelecimentos = StructType([
    StructField("cnpj_basico", StringType(), True),
    StructField("cnpj_ordem", StringType(), True),
    StructField("cnpj_dv", StringType(), True),
    StructField("identificador_matriz", IntegerType(), True),
    StructField("nome_fantasia", StringType(), True),
    StructField("situacao_cadastral", IntegerType(), True),
    StructField("situacao_cadastral_data", StringType(), True),
    StructField("situacao_cadastral_motivo_cod", IntegerType(), True),
    StructField("nome_cidade_exterior", StringType(), True),
    StructField("codigo_pais", StringType(), True),
    StructField("data_inicio_atividade", StringType(), True),
    StructField("cnae_fiscal_principal_cod", StringType(), True),
    StructField("cnae_fiscal_secundaria_cod", StringType(), True),
    StructField("endereço_tipo_logradouro", StringType(), True),
    StructField("endereço_logradouro", StringType(), True),
    StructField("endereço_numero", IntegerType(), True),
    StructField("endereço_complemento", StringType(), True),
    StructField("endereço_bairro", StringType(), True),
    StructField("endereço_cep", IntegerType(), True),
    StructField("endereço_uf", StringType(), True),
    StructField("endereço_cod_municipio", StringType(), True),
    StructField("contato_ddd1", IntegerType(), True),
    StructField("contato_telone1", IntegerType(), True),
    StructField("contato_ddd2", IntegerType(), True),
    StructField("contato_telefone2", IntegerType(), True),
    StructField("contato_ddd_fax", IntegerType(), True),
    StructField("contato_fax", IntegerType(), True),
    StructField("contato_email", StringType(), True),
    StructField("situacao_especial", StringType(), True),
    StructField("situacao_especial_data", StringType(), True)])


@F.udf(returnType=ArrayType(StringType()))
def _categoria_cnae(code1, code2):
    codes = []
    if code1 and code2:
        codes = (code1 + "," + code2).split(",")
    elif code1: 
        codes = [code1]
    elif code2:
        codes = code2.split(",")
    else:
        return None

    categoria = []
    for code in codes:
        try:
            first_2 = int(code[0:2])
        except: 
            raise Exception(code)
        if 1 <= first_2 <= 3:
            categoria.append("AGRICULTURA, PECUÁRIA, PRODUÇÃO FLORESTAL, PESCA E AQUICULTURA")
        elif 5 <= first_2 <= 9:
            categoria.append("INDÚSTRIAS EXTRATIVAS")
        elif 10 <= first_2 <= 33:
            categoria.append("INDÚSTRIAS DE TRANSFORMAÇÃO")
        elif 35 <= first_2 <= 35:
            categoria.append("ELETRICIDADE E GÁS")
        elif 36 <= first_2 <= 39:
            categoria.append("ÁGUA, ESGOTO, ATIVIDADES DE GESTÃO DE RESÍDUOS E DESCONTAMINAÇÃO")
        elif 41 <= first_2 <= 43:
            categoria.append("CONSTRUÇÃO")
        elif 45 <= first_2 <= 47:
            categoria.append("COMÉRCIO; REPARAÇÃO DE VEÍCULOS AUTOMOTORES E MOTOCICLETAS")
        elif 49 <= first_2 <= 53:
            categoria.append("TRANSPORTE, ARMAZENAGEM E CORREIO")
        elif 55 <= first_2 <= 56:
            categoria.append("ALOJAMENTO E ALIMENTAÇÃO")
        elif 58 == first_2:
            categoria.append("INFORMAÇÃO E COMUNICAÇÃO:EDIÇÃO E EDIÇÃO INTEGRADA À IMPRESSÃO")
        elif 59 == first_2:
            categoria.append("INFORMAÇÃO E COMUNICAÇÃO:ATIVIDADES CINEMATOGRÁFICAS, PRODUÇÃO DE VÍDEOS E DE PROGRAMAS DE TELEVISÃO; GRAVAÇÃO DE SOM E EDIÇÃO DE MÚSICA")
        elif 60 == first_2:
            categoria.append("INFORMAÇÃO E COMUNICAÇÃO:ATIVIDADES DE RÁDIO E DE TELEVISÃO")
        elif 61 == first_2:
            categoria.append("INFORMAÇÃO E COMUNICAÇÃO:TELECOMUNICAÇÕES")
        elif 62 == first_2:
            categoria.append("INFORMAÇÃO E COMUNICAÇÃO:ATIVIDADES DOS SERVIÇOS DE TECNOLOGIA DA INFORMAÇÃO")
        elif 63 == first_2:
            categoria.append("INFORMAÇÃO E COMUNICAÇÃO:ATIVIDADES DE PRESTAÇÃO DE SERVIÇOS DE INFORMAÇÃO")
        elif 64 <= first_2 <= 66:
            categoria.append("ATIVIDADES FINANCEIRAS, DE SEGUROS E SERVIÇOS RELACIONADOS")
        elif 68 <= first_2 <= 68:
            categoria.append("ATIVIDADES IMOBILIÁRIAS")
        elif 69 <= first_2 <= 75:
            categoria.append("ATIVIDADES PROFISSIONAIS, CIENTÍFICAS E TÉCNICAS")
        elif 77 <= first_2 <= 82:
            categoria.append("ATIVIDADES ADMINISTRATIVAS E SERVIÇOS COMPLEMENTARES")
        elif 84 <= first_2 <= 84:
            categoria.append("ADMINISTRAÇÃO PÚBLICA, DEFESA E SEGURIDADE SOCIAL")
        elif 85 <= first_2 <= 85:
            categoria.append("EDUCAÇÃO")
        elif 86 <= first_2 <= 88:
            categoria.append("SAÚDE HUMANA E SERVIÇOS SOCIAIS")
        elif 90 <= first_2 <= 93:
            categoria.append("ARTES, CULTURA, ESPORTE E RECREAÇÃO")
        elif 94 <= first_2 <= 96:
            categoria.append("OUTRAS ATIVIDADES DE SERVIÇOS")
        elif 97 <= first_2 <= 97:
            categoria.append("SERVIÇOS DOMÉSTICOS")
        elif 99 <= first_2 <= 99:
            categoria.append("ORGANISMOS INTERNACIONAIS E OUTRAS INSTITUIÇÕES EXTRATERRITORIAIS")
    
    cat = []
    # como aqui estamos considerando, na maioria dos casos, os grupos raiz de cada atividade, 
    # a lista de categoria pode conter grupos duplicados. Removemos os duplicados, preservando a
    # ordem. 
    for c in categoria:
        if c not in cat:
            cat.append(c)
    return cat