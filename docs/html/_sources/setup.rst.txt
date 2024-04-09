
******************************
Installation
******************************

The source code is currently hosted on GitHub at: https://github.com/lucasmsp/tlhop-library

The easiest way to install TLHOP Library is to install it from PIP.
This is the recommended installation method for most users.
The instructions for download and installing from source are also provided.


Installing from PIP
----------------------

The source code and the PIP package of TLHOP Library is available online at `Github <https://github.com/lucasmsp/tlhop-library>`_.
Users can compile the newest version using the following command::

    $ git clone https://github.com/lucasmsp/tlhop-library
    $ cd tlhop-library
    $ python3 setup.py sdist
    $ pip3 install dist/tlhop-library-<VERSION>.tar.gz
    # or by using bash gen-pip-package.sh to automatic generation a new pip package and to installing it

Another current option without installing it is by adding the tlhop-library folder in your PYTHONPATH environment::

    $ export PYTHONPATH=$PYTHONPATH:~/tlhop-library/

However, remember to make sure that all dependencies are satisfied by running::

    $ pip install -r requirements.txt


Environment variable
-----------------------

After installation, users must set an environment variable (`TLHOP_DATASETS_PATH`) to specify a folder where TLHOP can download new datasets or use it as a temporary folder. 
If more than one user uses the library, the directory path can be set up in a shared space.:

    $ export TLHOP_DATASETS_PATH="/opt/tlhop-datasets/"


Other dependencies: Apache Spark and Delta Lake format
--------------------------------------------------------

Because we use Python as a language interface to `Apache Spark <https://spark.apache.org/>`_, we can install Spark from `pip`, `conda` or any other Python's package installer. For instance, using a command like `pip install pyspark==3.3.1`. However, if you want to install PySpark in a distributed cluster, we recommend installing it manually (`reference <https://spark.apache.org/docs/latest/api/python/getting_started/install.html#manually-downloading>`_). In both cases, you may want to check in `Spark documentation page <https://spark.apache.org/docs/latest/#downloading>`_ if your environment already satisfies all dependencies like the supported Java and Python version.

`Delta Lake <https://delta.io/>`_ can also be partially installed from a package installer like pip (`pip install delta-spark=2.2.0`). We recommend setting a version when installing  to ensure that it will be compatible with your current Spark version. Please check the compatibility between Delta Lake and Spark versions before installing Delta Lake at `Delta documentation <https://docs.delta.io/latest/releases.html>`_. Besides the Python module, we recommend to download Delta Lake jars (`delta-core <https://search.maven.org/artifact/io.delta/delta-core_2.12>`_ and `delta-storage <https://search.maven.org/search?q=a:delta-storage>`_) to enable all features.

Finally, create or edit the `$SPARK_HOME/conf/spark-defaults.conf`::


    spark.sql.parquet.compression.codec gzip
    # DELTA LAKE
    spark.driver.extraClassPath /opt/spark/jars/delta-core_2.12-2.2.0.jar:/opt/spark/jars/delta-storage-2.2.0.jar
    spark.submit.pyFiles /opt/spark/jars/delta-core_2.12-2.2.0.jar
    spark.sql.extensions io.delta.sql.DeltaSparkSessionExtension
    spark.sql.catalog.spark_catalog org.apache.spark.sql.delta.catalog.DeltaCatalog
