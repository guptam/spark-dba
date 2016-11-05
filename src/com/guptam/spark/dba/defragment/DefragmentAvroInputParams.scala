
/*
* Author: Manish Gupta
*/

package com.guptam.spark.dba.defragment

import scopt.OptionParser
import com.guptam.spark.dba.common._

trait DefragmentAvroInputParams {
  // This is how the JAR arguments be supplied
  case class InputConfiguration(
    sourceFolder: String = "",
    targetFolder: String = "",
    avroSchema: String = "",
    fileCount: Int = 0,
    hadoopConfigPath: String = "",
    runningLocally: Boolean = false,
    overwriteTarget: Boolean = false,
    tmpFolder: String = "",
    trashFolder: String = "") {}

  // Get a new Option Parser
  def getNewInputConfigurationParser(): OptionParser[this.InputConfiguration] = {
    new scopt.OptionParser[InputConfiguration](Constants.DEFRAG_AVRO_CLASS_NAME) {
      head(Constants.DEFRAG_AVRO_CLASS_NAME, "1.0")
      opt[String]("sourceFolder")
        .required()
        .valueName("<Source Folder Path>")
        .action((x, c) => c.copy(sourceFolder = x))
        .text("(String)(Required) Should be a valid folder containing "
          + "AVRO files (file names must have .avro as an extension)")
      opt[String]("targetFolder")
        .required()
        .valueName("<Target Folder Path>")
        .action((x, c) => c.copy(targetFolder = x))
        .text("(String)(Required) Target Folder for collecting merged avro files. "
          + "Folder will be recreated, so it should not exist. ")
      opt[String]("avroSchema")
        .optional()
        .valueName("<Avro Schema File>")
        .action((x, c) => c.copy(avroSchema = x))
        .text("(File) AVSC i.e. avro schema file. If not provided, then Defragmenter "
          + "will use the schema embedde in avro files. ")
      opt[Int]("fileCount")
        .optional()
        .valueName("<Final File Count>")
        .action((x, c) => c.copy(fileCount = x))
        .text("(Int) Number of files to create in Target Folder. This utility is "
          + "based on Spark coalesce")
      help("help").text("prints the usage text")
      opt[String]("hadoopConfigPath")
        .optional()
        .valueName("<Folder Path to Hadoop Config Files>")
        .action((x, c) => c.copy(hadoopConfigPath = x))
        .text("(String) Folder in which Hadoop Config - core-site.xml "
          + "and hdfs-site.xml exists")
      opt[Unit]("runningLocally")
        .optional()
        .action((_, c) => c.copy(runningLocally = true))
        .text("To be used only when running in eclipse")
      opt[Unit]("overwriteTarget")
        .optional()
        .action((_, c) => c.copy(overwriteTarget = true))
        .text("Overwrite Target (Target will be deleted on succesfull compltetion only)")
      opt[String]("tmpFolder")
        .optional()
        .valueName("<Temporary Folder Path>")
        .action((x, c) => c.copy(tmpFolder = x))
        .text("(String) Temporary Folder for saving the final output. Once the "
          + "job complete succesfully, this folder will overwrite target. ")
      opt[String]("trashFolder")
        .optional()
        .valueName("<Trash Folder Path>")
        .action((x, c) => c.copy(trashFolder = x))
        .text("(String) Trash Folder for moving the target folder before being "
          + "overwritten by tmp. Trash can be cleaned up regularly.")
    }
  }
}
