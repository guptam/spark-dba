
/*
* Author: Manish Gupta
*/

package com.guptam.spark.dba.hdfsutil

import org.apache.avro.mapred.FsInput
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{ GenericDatumReader, GenericRecord }
import org.apache.hadoop.fs.{ Path, FileSystem, FileStatus }

import scala.collection.mutable.ListBuffer
import com.guptam.spark.dba.common._

class HDFSAvroOperations(sPathToConfig: String) extends HadoopFileSystemOperation(sPathToConfig) {

  // This can be very slow
  def listAllAvroFolders(location: String, recursive: Boolean): List[String] = {
    val inputLocation = getResolvedPath(location).toString()
    val lst = fileSystem.listFiles(getPath(location), true)
    val result = new ListBuffer[String]()
    while (lst.hasNext()) {
      val p = lst.next()
      if (p.isFile() && isValidAvroFile(p.getPath())) {
        val parent = getPath(p.getPath.getParent.toString()).toString()
        if (!result.contains(parent.toString)) {
          result += parent.toString
        }
      }
    }
    result.toList
  }

  // count total number of avro files in a folder
  // ignore . and _ files
  def totalAvroFilesToProcess(sourceFolder: String): Int = {
    getFileStatistics(sourceFolder) match {
      case Some(f) => f.count({ fileStatus => isValidAvroFile(fileStatus.getPath())
      })
      case None => 0
    }
  }

  // Get Schema From an Avro File
  def getSchemaFromAvroFile(filename: String): String = {
    val fsi = new FsInput(getPath(filename), conf)
    val dr = DataFileReader.openReader(fsi, new GenericDatumReader[GenericRecord]())
    val resolvedSchema = dr.getSchema.toString()
    dr.close()
    fsi.close()
    resolvedSchema
  }

  // either all are sub directories or all are avro files
  def isValidAvroDirectory(location: String): Boolean = {
    var countAvroFile, countNonAvroFile, countDirectories: Int = 0
    getFileStatisticsExcludingDotUnderscore(location) match {
      case Some(fs) =>
        countAvroFile = fs.count(fileStatus => fileStatus.isFile() &&
          isValidAvroFile(fileStatus.getPath()))
        countNonAvroFile = fs.count(fileStatus => fileStatus.isFile() &&
          !isValidAvroFile(fileStatus.getPath()))
        countDirectories = fs.count(fileStatus => fileStatus.isDirectory())
      case None => countAvroFile = 0; countNonAvroFile = 0; countDirectories = 0
    }

    if ((countAvroFile == 0 && countNonAvroFile == 0 && countDirectories == 0) ||
      (countNonAvroFile > 0) ||
      (countAvroFile > 0 && countDirectories > 0)) {
      false // Folder is empty or contain non avro file or contain both file and directories
    } else {
      true
    }
  }

  // either all are sub directories or all are avro files
  def isValidAvroRootDirectory(location: String): Boolean = {
    getFileStatistics(location) match {
      case Some(fs) =>
        if ((fs.find(fileStatus => {
          // ignore files/dir starting with . or _
          ( !isIgnoreFile(fileStatus.getPath()) ) &&
            (
              fileStatus.isDirectory()
              ||
              (fileStatus.isFile() && !isValidAvroFile(fileStatus.getPath()))
            )
        }) != None) || fs.isEmpty) // able to find or folder is empty
        {
          false // not a valid folder for Avro Defragmentation
        } else {
          true // Valid folder for Avro Defragmentation
        }
      case None => false
    }

  }

  // Size in bytes of all .avro files
  def getAvroDataSize(location: String): Long = {
    if (isValidAvroDirectory(location)) {
      fileSystem.listStatus(getPath(location))
        .filter({ fileStatus => isValidAvroFile(fileStatus.getPath())
        })
        .map(fileStatus => fileSystem.getContentSummary(fileStatus.getPath()).getSpaceConsumed())
        .sum
    } else {
      0.toLong
    }
  }

  // Get File name with maximum modification time
  def getLatestModifiedAvroFile(location: String): Option[String] = {
    if (isValidAvroDirectory(location)) {
      val listContentSummary =
        fileSystem.listStatus(getPath(location))
          .filter({ fileStatus => isValidAvroFile(fileStatus.getPath())
          })
          .zipWithIndex
      Some(listContentSummary(listContentSummary
        .maxBy(_._1.getModificationTime)._2)._1.getPath().toString())
    } else { None }
  }


    // If the File is an Avro File
  def isValidAvroFile(location: Path): Boolean = {
    val strLocation = location.getName().toLowerCase()
    if (strLocation.endsWith(Constants.AVRO_FILE_EXTENSION) &&
        !isIgnoreFile(location) ) {
      true
    } else {
      false
    }
  }


}
