
/*
* Author: Manish Gupta
*/

// scalastyle:off println
package com.guptam.spark.dba.defragment

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.avro.Schema

import com.guptam.spark.dba.hdfsutil.HDFSAvroOperations
import java.io.InputStream

// For Avro Processing (newHadoopAPI)
// Can not use spark-avro for reading, because it does not support schema evolution
// spark-avro extracts the schema from one of the random files in HDFS
// and use that for all the Avro reading and writing
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.fs.{ FileStatus, Path }
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.avro.generic.{ GenericDatumReader, GenericRecord }
import org.apache.avro.mapred.{ AvroKey, FsInput }
import org.apache.avro.mapreduce.{ AvroJob, AvroKeyInputFormat, AvroKeyOutputFormat }
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import scala.collection.mutable.ListBuffer

import org.apache.avro.mapred.FsInput
import com.guptam.spark.dba.common._
import com.guptam.spark.dba.hdfsutil.HDFSAvroOperations

/* *****************************************************************************
 * This Avro Folder Defragmenter takes multiple small avro files of same or
 * compatible schema and merge all of them together in lesser number of larger
 * files (which is more suitable for Hadoop).
 * Some of the features of this Defragmenter are:
 *  - Ability to overwrite the source folder itself
 *  - Moves the target to a trash folder if overwrite is enabled
 *  - can work on an external avro schema
 *  - Defragmenter make sure right before writing the target that state of source is exactly same
 *    as it was when the job first started reading the data for defragmentation.
 *  - Source should have atleast 2 Avro files to work with
 ***************************************************************************** */
object DefragmentAvroFolder extends DefragmentAvroInputParams {

  // custom logger
  val myLogger: CustomLogger = new CustomLogger
  def logme(message: Any, category: String, ThreadName: String): Unit = {
    myLogger.logme(message, category, ThreadName)
  }
  def loginfo(message: Any, ThreadName: String = Constants.PARENT): Unit = {
    logme(message, Constants.INFO, ThreadName)
  }
  def logerror(message: Any, ThreadName: String = Constants.PARENT): Unit = {
    logme(message, Constants.ERROR, ThreadName)
  }

  // Main function
  def main(args: Array[String]): Unit = {

    // Parse command line arguments
    val parser = getNewInputConfigurationParser()

    // If succeed in parsing options, then call run
    parser.parse(args, InputConfiguration()) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }
  }

  /* *********************************************************************
   * This fetches the Avro schema:
   * - if user has provided an external schema, then it returns schema from
   * that avsc file.
   * - if user has not provided any avro schema file, then this method extracts
   * avro schema from latest file in folder.
   * ## This will fail for a partitioned folder ##
   * *********************************************************************
   * * */
  def getAvroSchema(hdfs: HDFSAvroOperations, config: InputConfiguration): Option[Schema] = {
    val schema: Option[Schema] = {
      loginfo("Validating: If Schema File is provided, set Schema, else leave it null")
      if (!config.avroSchema.isEmpty()) {
        if (hdfs.isValidFile(config.avroSchema)) {
          loginfo("Loading external avro schema")
          Some(new Schema.Parser().parse(hdfs.getFile(config.avroSchema)))
        } else { None } // Location of schema file is invalid on FileSystem
      } else { // User has not provided with any Schema File
        // try to find the avro schema from latest file inside the folder
        // latest file is the one whose modification time is largest
        // assumption is: Latest file will have latest schema
        loginfo("Loading avro schema from latest avro file in folder")
        val SchemaFromLatestAvroFile = hdfs.getLatestModifiedAvroFile(config.sourceFolder) match {
          case Some(lma) =>
            loginfo("Latest Avro File is: " + lma)
            Some(new Schema.Parser().parse(hdfs.getSchemaFromAvroFile(lma)))
          case None => None
        }
        SchemaFromLatestAvroFile
      }
    }
    loginfo("Schema is: " + schema.toString())
    schema
  }

  /* *********************************************************************
   * Validation method for each partition folder. This make sure that folder
   * only has avro files, and no sub directory and no other file type.
   * It skips looking at files that start with . or _
   * *********************************************************************
   * * */
  def validatePartitionedAvroFolder(
    hdfs: HDFSAvroOperations,
    partitionedFolder: String,
    partitionPath: String): Option[String] = {
    try {

      // if the source does not exists, quit
      loginfo("Validating: Source Folder", partitionPath)
      if (!(hdfs.isValidAvroDirectory(partitionedFolder))) {
        throw new IllegalArgumentException("Source Folder is not correct. Please ensure that folder only has .avro files and no sub directory.")
      }

      // counting avro files in source
      val countInputFile = hdfs.totalAvroFilesToProcess(partitionedFolder)
      loginfo("Total Avro Files in Source Folder = " + countInputFile.toString(), partitionPath)
      if (countInputFile < 2) {
        throw new IllegalArgumentException("Insufficient Avro Files in Source to merge")
      }

      // Nothing failed
      None
    } catch {
      case someError: Throwable => Some(someError.toString())
    }
  }

  /* *********************************************************************
   * Validation method for checking the top level folder. It only validates
   * the existence of various folders based on input config
   * *********************************************************************
   * * */
  def validateEverythingAtRootLevel(
    hdfs: HDFSAvroOperations,
    config: InputConfiguration): Option[String] = {
    try {

      // if the source does not exists, quit
      loginfo("Validating: Source Folder")
      if (!(hdfs.isValidAvroDirectory(config.sourceFolder))) {
        throw new IllegalArgumentException("Source Folder is not correct. Please "
          + "ensure that source folder only has .avro files and no sub directory.")
      }

      // if the target already exists, quit
      loginfo("Validating: If Overwrite is disabled then Target Folder should not exist")
      if (hdfs.pathExists(config.targetFolder) && (config.overwriteTarget == false)) {
        throw new IllegalArgumentException("Target Folder Already exists.")
      }

      // if overwrite is enabled then trash folder is mandatory
      loginfo("Validating: If Overwrite is enabled then Trash Folder must exist")
      if ((config.overwriteTarget == true) && (!hdfs.pathExists(config.trashFolder))) {
        throw new IllegalArgumentException("Trash Folder is mandatory if Overwrite is enabled.")
      }

      // if overwrite is enabled then temp folder is mandatory
      loginfo("Validating: If Overwrite is enabled then Tmp Folder must exist")
      if ((config.overwriteTarget == true) && (!hdfs.pathExists(config.tmpFolder))) {
        throw new IllegalArgumentException("Tmp Folder is mandatory if Overwrite is enabled.")
      }

      // Nothing failed
      None
    } catch {
      case someError: Throwable => Some(someError.toString())
    }
  }

  // return path of root tmp folder
  def getTempSessionFolder(rootTmpFolder: String, sessionIdentifier: String): String = {
    rootTmpFolder + "/_tmp_" + sessionIdentifier
  }

  /* *********************************************************************
   * Identify the target folder for each partition
   * *********************************************************************
   */
  def jobImmediateOutputPath(
    hdfs: HDFSAvroOperations,
    config: InputConfiguration,
    partitionPath: String,
    sessionIdentifier: String): String = {
    if (config.overwriteTarget) {
      // if overwrite is true and user has provided a Tmp directory
      getTempSessionFolder(hdfs.removeLastSlash(config.tmpFolder), sessionIdentifier) + partitionPath
    } else {
      // overwrite is false
      config.targetFolder + partitionPath
    }
  }

  /* *********************************************************************
   * Initialize a new MRJob object for each source and target folder
   * *********************************************************************
   */
  def getAvroMapReduceJob(hdfs: HDFSAvroOperations,
                          schema: Option[Schema],
                          inputFolder: String,
                          outputFolder: String): Job = {

    val mrJob = new Job()

    FileInputFormat.setInputPaths(mrJob, inputFolder)
    FileInputFormat.setInputDirRecursive(mrJob, true)

    // if Schema is available, then use it
    if (!schema.isEmpty) {
      AvroJob.setInputKeySchema(mrJob, schema.get)
      AvroJob.setOutputKeySchema(mrJob, schema.get)
    }

    // Assuming final RDD is finalRDD (after all the transformations)
    // Save to Output folder using predefined schema

    FileOutputFormat.setOutputPath(mrJob, hdfs.getPath(outputFolder))
    FileOutputFormat.setCompressOutput(mrJob, true)
    FileOutputFormat.setOutputCompressorClass(
      mrJob, classOf[org.apache.hadoop.io.compress.SnappyCodec])
    mrJob.setOutputFormatClass(classOf[AvroKeyOutputFormat[GenericRecord]])

    // return the job object
    mrJob
  }

  // Wrapper for all the defragmentation logic for a single Avro Folder
  def processSingleAvroFolder(
    hdfs: HDFSAvroOperations,
    config: InputConfiguration,
    sparkContext: SparkContext,
    schema: Option[Schema],
    partitionPath: String,
    sessionIdentifier: String): Option[String] = {

    try {
      val fullPath: String = config.sourceFolder + partitionPath

      // validate individual folder
      validatePartitionedAvroFolder(hdfs, fullPath, partitionPath) match {

        case Some(errorString: String) =>
          logerror(errorString, partitionPath)
          throw new IllegalArgumentException(errorString)
        case None =>
          // Immediate Output Folder
          val outputFolder: String = jobImmediateOutputPath(hdfs, config, partitionPath, sessionIdentifier)
          loginfo("Output Folder for Merged RDD is: " + outputFolder, partitionPath)

          // Get MR Job object
          loginfo("Setting mrJob, InputFormat, OutputFormat", partitionPath)
          val mrJob = getAvroMapReduceJob(hdfs, schema, fullPath, outputFolder)

          loginfo("Reading Avro Folder using the predefined schema", partitionPath)
          val avroRDD = sparkContext.newAPIHadoopRDD(
            mrJob.getConfiguration(),
            classOf[AvroKeyInputFormat[GenericRecord]],
            classOf[AvroKey[GenericRecord]],
            classOf[NullWritable])
            .cache

          // This is to checkpoint the state of source.
          // Will be used only on source Overwrite
          val sourceFileListingAtCheckpoint: Option[Array[FileStatus]] = hdfs.getFileStatistics(fullPath)

          loginfo("Total Avro Data Size is: " + hdfs.getAvroDataSize(fullPath).toString() + " bytes", partitionPath)

          // -----------------------------------------------------------------------------------------
          // Place to do any ETL (like filtering) on avroRDD ---------------------------------------
          // -----------------------------------------------------------------------------------------
          loginfo("Merging " + (sourceFileListingAtCheckpoint match {
            case Some(f) => f.count(fileStatus => hdfs.isValidAvroFile(fileStatus.getPath()))
            case None => 0
          }).toString()
            + " Avro File in source to " + config.fileCount.toString() + " Avro Files in Target", partitionPath)

          // Final RDD to be written
          val finalRDD = avroRDD.coalesce(config.fileCount)

          val outCount = finalRDD.count
          loginfo("Defragmentation Job will write " + outCount + " avro records.", partitionPath)
          // -----------------------------------------------------------------------------------------
          // -----------------------------------------------------------------------------------------

          loginfo("Saving Data in output Folder", partitionPath)
          finalRDD.saveAsNewAPIHadoopDataset(mrJob.getConfiguration())

          loginfo("Validating: File Statistics (with checkpoint)", partitionPath)
          if (hdfs.equalsFileStatistics(sourceFileListingAtCheckpoint.get, hdfs.getFileStatistics(config.sourceFolder + partitionPath).get)) {
            loginfo("File Statistics unchanged. Initiating Merge.", partitionPath)

            // merge temp to target
            if (config.overwriteTarget) {

              // Trash Folder Path
              val trashedFolderName = hdfs.removeLastSlash(config.trashFolder) + "/" + hdfs.getPath(config.targetFolder).getName() + "_" + sessionIdentifier + partitionPath

              // Move Target to trash
              hdfs.renameDirectory(config.targetFolder + partitionPath, trashedFolderName)
              loginfo("Trashed Folder " + config.targetFolder + partitionPath + " -> " + trashedFolderName, partitionPath)

              // Move temp to Target
              hdfs.renameDirectory(outputFolder, config.targetFolder + partitionPath)
              loginfo("Moved Temporary Folder: " + outputFolder + " -> " + config.targetFolder + partitionPath, partitionPath)

            }
          } else {
            logerror("File Statistics changed. Aborting Merge.", partitionPath)
          }
      }
      None
    } catch {
      case someError: Throwable =>
        logerror(someError.toString(), partitionPath)
        throw (someError)
    }

  }

  // called from main
  def run(config: InputConfiguration): Unit = {
    try {
      logme("Starting Defragmentation for config: " + config.toString(), Constants.STARTING, Constants.PARENT)

      // HDFS operations - only validations
      loginfo("Initiating Hadoop File System")
      val hdfs = new HDFSAvroOperations(config.hadoopConfigPath)

      // Try to get schema File
      val schema = getAvroSchema(hdfs, config)

      // kick-off all the basic validations
      // proceed only on success
      validateEverythingAtRootLevel(hdfs, config) match {
        case Some(errorString) =>
          logerror(errorString)
          throw new IllegalArgumentException(errorString)
        case None =>
          // initiate a spark context
          loginfo("Set Spark conf and set Kryo Serializer")
          val conf = new SparkConf().setAppName(Constants.DEFRAG_AVRO_CLASS_NAME)

          // We need to specify a serializer because Avro Key is not serializable by default
          conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

          // If running in local mode
          if (config.runningLocally) {
            loginfo("Running in Local Mode")
            conf.setMaster("local[*]")
          }

          // Initialize SparkSQL context
          loginfo("Start Spark Context")
          val sparkContext = new SparkContext(conf)

          /* Get List of folders to process
           * if the folder is partitioned, then this will return a
           * leaf level path (directly containing the avro files.
           * If the folder is root, only root folder is returned.
           * Changed: Now it only contains relative path of partition
           */
          val lstSourcefolders = hdfs.listAllAvroFolders(config.sourceFolder, true).map(
            x => {
              hdfs.getPartitionPath(config.sourceFolder, x)
            })

          val sessionIdentifier = hdfs.getUniqueHashAndTime()
          loginfo("Session Identifier: " + sessionIdentifier)

          loginfo("Partitions to process: " + lstSourcefolders.mkString(Constants.COMMA))
          // for each folder in parallel
          lstSourcefolders.par.foreach(partitionPath =>
            {
              try {
                loginfo("Starting", partitionPath)
                processSingleAvroFolder(hdfs, config, sparkContext, schema, partitionPath, sessionIdentifier)
                logme(Constants.SUCCESS, Constants.SUCCESS, partitionPath)
              } catch {
                case e: Throwable => logerror(e, partitionPath)
              }
            })

          // Stop Spark Context
          loginfo("Stopping Spark Context")
          sparkContext.stop()

          // Log success and cleanup
          if (!myLogger.containsError()) {
            val sessionTempFolder = getTempSessionFolder(config.tmpFolder, sessionIdentifier)
            if (!sessionTempFolder.isEmpty() && sessionTempFolder.contains(sessionIdentifier)) {
              loginfo("Cleaning up the Temporary Folder: " + sessionTempFolder)
              hdfs.deleteFolder(sessionTempFolder, true)
            }
            logme(Constants.SUCCESS, Constants.SUCCESS, Constants.PARENT)
          } else {
            logme("Errors in defragmenting one or more Partitions", Constants.ERROR, Constants.PARENT)
          }

      }

      // Write everything in a log file inside the target folder
      hdfs.createAndWriteFileFromListBuffer(
        hdfs.removeLastSlash(config.targetFolder) + "/.defraglog",
        myLogger.getCompleteLogs().toList,
        true)

    } catch {
      case someError: Throwable =>
        logerror(someError)
    } finally {
      logme(Constants.FINALLY, Constants.FINALLY, Constants.PARENT)
    }

  }
}
