Converters
***********

We made available the ShodanDatasetManager and CensysDatasetManager, both class to convert multiples Shodan (JSON) or Censys (Avro) files into a single Delta format, to enrich and manage others aspects of the final dataset. The :doc:`API Reference <api>` page describes all available methods. To create the dataset, run the method `convert_files` as shown below:

.. code-block:: python
   :linenos:
   
   from tlhop.converters import ShodanDatasetManager
   shodan_mgr = ShodanDatasetManager(org_refinement=True, fix_brazilian_cities=True)
   shodan_mgr.convert_files(INPUT_FILES, output_folder=DEST_FOLDER)

   from tlhop.converters import CensysDatasetManager
   censys_mgr = CensysDatasetManager(filter_by_country="Brazil")
   censys_mgr.convert_files(INPUT_SNAPSHOT_FOLDER, output_folder=DEST_FOLDER)
    

After that, the dataset can be read as any other Spark supported format. 

.. code-block:: python
   :linenos:
   
   df = spark.read.format("delta").load(DEST_FOLDER)



Dataset optimizations
------------------------

Spark may generate small files over time. Because of that, we also expose a method (`optimize_delta`) to optimize the dataset by merging small files into a bigger size.

.. code-block:: python
   :linenos:
   
    shodan_mgr.optimize_delta(DEST_FOLDER)


Delta format supports time travel. In order to support this feature, older files version are kept inside dataset folder. When we ensure that older dataset versions are not needed anymore, we can use the `remove_old_delta_versions` method to force a removal of these old versions.

.. code-block:: python
   :linenos:
   
    shodan_mgr.remove_old_delta_versions(DEST_FOLDER)



**Obs:** The complete demonstration is available at `conversion_api_shodan.ipynb <https://github.com/lucasmsp/tlhop-library/tree/main/examples/conversion_api_shodan.ipynb>`_ and `conversion_api_censys.ipynb <https://github.com/lucasmsp/tlhop-library/tree/main/examples/conversion_api_censys.ipynb>`_ notebooks. 