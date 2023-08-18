# TLHOP Library

<h3 align="center">A library designed to process IoT search engines like Shodan for Big Data scenarios.</h3>

<p align="center"><b>
    <a href="https://lucasmsp.github.io/tlhop-library">Documentation</a> •
    <a href="https://github.com/lucasmsp/tlhop-library/blob/main/RELEASE_NOTES">Releases</a>
</b>

</p>

This library is developed by TLHOP (Thread-Limiting Holistic Open Platform) project Team, a technical-scientific project signed between the Centro de Estudos, Resposta e Tratamento de Incidentes de Segurança no Brasil (CERT.br) and the Universidade Federal de Minas Gerais (UFMG) to understand and analyze the network security of the Brazilian Internet.

In this project's repository, we developed a robust and scalable execution environment using Apache Spark, capable of executing complex queries on large volumes of data in a semi-iterative way from a programmable interface. We have developed several techniques to debug Shodan's data and aggregated external databases (such as Whois, CAIDA ASes rankings and the NVD) to complement the information collected by Shodan. Our initial data analysis identified interesting security properties in Brazilian networks.


## Environment description and configuration:

The two main tools we use in the project are: 

1. [Apache Spark][apache-spark] for processing large volumes of data like from Shodan (using Python Language); 
2. [Delta][delta] used as an efficient data storage format.

[apache-spark]: https://spark.apache.org/
[delta]: https://delta.io/


### Apache Spark and Delta Lake format


Because we use Python as a language interface to Apache Spark, we can install Spark from `pip`, `conda` or any other Python's package installer. For instance, using a command like `pip install pyspark==3.3.1`. However, if you want to install PySpark in a distributed cluster, we recommend installing it manually ([reference][spark-manual]). In both cases, you may want to check in [Spark documentation page][spark-download] if your environment already satisfies all dependencies like the supported Java and Python version.

Delta Lake can also be partially installed from a package installer like pip (`pip install delta-spark=2.2.0`). We recommend setting a version when installing  to ensure that it will be compatible with your current Spark version. Please check the compatibility between Delta Lake and Spark versions before installing Delta Lake at [Delta documentation][delta-matrix]. Besides the Python module, we recommend to download Delta Lake jars ([delta-core][delta-core] and [delta-storage][delta-storage]) to enable all features.


[spark-download]: https://spark.apache.org/docs/latest/#downloading
[spark-manual]: https://spark.apache.org/docs/latest/api/python/getting_started/install.html#manually-downloading
[delta-matrix]: https://docs.delta.io/latest/releases.html
[delta-core]: https://search.maven.org/artifact/io.delta/delta-core_2.12
[delta-storage]: https://search.maven.org/search?q=a:delta-storage


Finally, create or edit the `$SPARK_HOME/conf/spark-defaults.conf`: 

```
spark.sql.parquet.compression.codec gzip
# DELTA LAKE
spark.driver.extraClassPath /opt/spark/jars/delta-core_2.12-2.2.0.jar:/opt/spark/jars/delta-storage-2.2.0.jar
spark.submit.pyFiles /opt/spark/jars/delta-core_2.12-2.2.0.jar
spark.sql.extensions io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog org.apache.spark.sql.delta.catalog.DeltaCatalog
```

### Jupyter Lab and Python dependencies

Jupyter Lab and all others Python dependencies can be also installed from any Python's package installer. All dependencies are described in `requirements.txt` as the follows:

```
graphqlclient>=0.2.4
jupyterlab
matplotlib>=3.4.3
nltk>=3.7
numpy>=1.21.1
pandas>=1.3.1
plotly>=5.6.0
pyarrow>=5.0.0
scikit-learn>=1.1.2
sphinx>=5.3.0
sphinx-rtd-theme>=1.1.1
tabulate>=0.9.0
textdistance>=4.2.2
```

When using pip, one can install all dependencies using command `pip install -f requirements.txt`.
