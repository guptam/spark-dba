# spark-dba
Collection of utilities for managing data on Hadoop powered by Apache Spark.
#### To build: Add Spark SDK as external Jar
##### Tested on HDInsight cluster using JAR at http://go.microsoft.com/fwlink/?LinkID=723585&clcid=0x409.



### 1. DefragmentAvroFolder (Defragment Avro Folder on HDFS using Apache Spark)
-------------------------------------------------------------------------------
This Avro Folder Defragmenter takes multiple small avro files of same or compatible schema and merge all of them together in lesser number of larger files (which is more suitable for Hadoop).

Some of the features of this Defragmenter are:

* Ability to overwrite the source folder itself (just specify target path same as source folder)
* Moves the target to a trash folder if overwrite is enabled
* Can work on an external avro schema. If Avro Schema file is not mentioned, then this utility picks the avro schema from latest avro file in the folder (the file with maximum Last modification date). New files (after defrag) gets the new Avro Schema.
* Defragmenter make sure right before writing the target that state of source is exactly same as it was when the job first started reading the data for defragmentation. If there is any modification in source after checkpoint, process abort the final target overwrite. 
* Source should have atleast 2 Avro files to work with.
* Ability to work on partitioned (multilevel) folder. Target folder will be created with the same folder structure.

** In Progress: Logic to calculate the number of partitions dynamically based on data size

#### Help output:
DefragmentAvroFolder 1.0
Usage: DefragmentAvroFolder [options]

  --sourceFolder <Source Folder Path>
                           (String)(Required) Should be a valid folder containing AVRO files (file names must have .avro as an extension)
  --targetFolder <Target Folder Path>
                           (String)(Required) Target Folder for collecting merged avro files. Folder will be recreated, so it should not exist. 
  --avroSchema <Avro Schema File>
                           (File) AVSC i.e. avro schema file. If not provided, then Defragmenter will use the schema embedde in avro files. 
  --fileCount <Final File Count>
                           (Int) Number of files to create in Target Folder. This utility is based on Spark coalesce
  --help                   prints the usage text
  --hadoopConfigPath <Folder Path to Hadoop Config Files>
                           (String) Folder in which Hadoop Config - core-site.xml and hdfs-site.xml exists
  --runningLocally         To be used only when running in eclipse
  --overwriteTarget        Overwrite Target (Target will be deleted on succesfull compltetion only)
  --tmpFolder <Temporary Folder Path>
                           (String) Temporary Folder for saving the final output. Once the job complete succesfully, this folder will overwrite target. 
  --trashFolder <Trash Folder Path>
                           (String) Trash Folder for moving the target folder before being overwritten by tmp. Trash can be cleaned up regularly.



####Sample command line arguments for spark-submit (DefragmentAvroFolder):
##### 1.1. Source and Target can be same (Source will be overwritten at the end (moved to trash first) ):
--sourceFolder /data/AvroFolderCompactor/avrodata --targetFolder /data/AvroFolderCompactor/avrodata 
--avroSchema /data/AvroFolderCompactor/schema/airline.avsc --fileCount 2 --runningLocally --overwriteTarget 
--trashFolder /data/AvroFolderCompactor/trash --tmpFolder /data/AvroFolderCompactor/tmp

##### 1.2. Source and Target can be diffrent (Target will be overwritten at the end (moved to trash first)):
--sourceFolder /data/AvroFolderCompactor/source/avrodata --targetFolder /data/AvroFolderCompactor/target/avrodata 
--avroSchema /data/AvroFolderCompactor/schema/airline.avsc --fileCount 2 --runningLocally --overwriteTarget 
--trashFolder /data/AvroFolderCompactor/trash --tmpFolder /data/AvroFolderCompactor/tmp