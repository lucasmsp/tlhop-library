TLHOP abstraction
*******************

TLHOP abstraction is an extended Spark's DataFrame abstraction that contains high-level methods for handling data from networks probed devices like Shodan's to bring the programming language closer to network/security analysts and operators. Currently, more than 15 operators are available, ranging from formatting records with fields containing images to standardizing texts or standardizing organization names.  Our abstraction is useful because although the functions provided by platforms such as Spark can be used in most of our activities, there are still frequent tasks, linked to a specific domain activity, that we repeatedly need to provide as an User-Defined Function (UDF), for example, to structure the fields of CPE (Common Platform Enumeration) codes. There are also scenarios where, although Spark already provides the necessary operators for an activity, it still requires complex logic involving two or more operators aligned that make the code creation process slower. The below code exemplifies one of implemeted operation and how to use it for filtering records from a list of CVEs:

.. code-block:: text

   # native approach:
   import pyspark.sql.functions as F

   target = ["CVE-2021-26855", ..., "CVE-2021-26885"]
   tmp = F.array(*[F.lit(c) for c in target])
   df.filter(F.arrays_overlap(“column_name”, tmp))

   # using our abstraction:
   from tlhop.tlhop_abstraction import DataFrame

   target = ["CVE-2021-26855", ..., "CVE-2021-26885"]
   df.shodan_extension.contains_vulnerability(target, mode="any")

While using only the Spark functions, it would demand a greater technical knowledge from the user, in this case, it would be necessary to use four other functions for the same task. Furthermore, if we decide to filter only records with all vulnerabilities (instead of at least one) the needed code will change significantly.

Another example of an operator is `gen_meta_events`, responsible for generating a new column (called *meta-events*) containing a list of labels that identify important items found in each record. In our research, we implemented 28 item signatures, for example, if a record has any vulnerability, a label "has_vulns" will be applied or if it has screenshots of images, it may also present the label "has_screenshots", making it easier to understand the data. This logic is also capable of identifying more complex patterns, such as, a malware pattern that can be traced from the fields that Shodan already collects or by new events that can be extended by the user.

The provided operators act as a wrapper for more complex internal code. In this way, more frequent code snippets or aligned operations can be easily transformed into a new operator to speed up the development of new analyses. Once the abstraction is imported, Spark's native operators can be executed together with the extended ones transparently to the user.

------------

**Obs:** Other available operations are document in :doc:`API Reference <api>` page.