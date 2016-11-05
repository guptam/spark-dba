
/*
* Author: Manish Gupta
*/

package com.guptam.spark.dba.hdfsutil

import java.io.BufferedInputStream
import java.io.FileInputStream
import java.io.InputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ Path, FileSystem, FileStatus }
import scala.collection.mutable.ListBuffer
import com.guptam.spark.dba.common._

class HadoopFileSystemOperation(sPathToConfig: String) extends HDFSStringUtil {

  // Create a new Hadoop Conf and get a FileSystem object
  val conf: Configuration = new Configuration()
  if (!sPathToConfig.isEmpty()) {
    conf.addResource(new Path(removeLastSlash(sPathToConfig) + "/core-site.xml"))
    conf.addResource(new Path(removeLastSlash(sPathToConfig) + "/hdfs-site.xml"))
  }

  // File system object
  val fileSystem = FileSystem.get(conf)

  // Write a list of string to file on hdfs
  def createAndWriteFileFromListBuffer(
    fileName: String,
    data: List[String],
    overwrite: Boolean): Unit = {
    val os = fileSystem.create(getPath(fileName), overwrite)
    os.writeBytes(data.mkString("\n"))
    os.close()
  }

  // rename a the directory
  def renameDirectory(from: String, to: String): Boolean = {
    if (isValidDirectory(from)) {
      fileSystem.rename(getPath(from), getPath(to)) // return true is succesfull
    } else {
      false
    }
  }

  // Get listing of all files and statistics
  def getFileStatisticsExcludingDotUnderscore(location: String): Option[Array[FileStatus]] = {
    if (isValidDirectory(location)) {
      Some(fileSystem.listStatus(getPath(location)).filter(fileStatus =>
        !(fileStatus.getPath().getName().startsWith(Constants.UNDERSCORE)
          || fileStatus.getPath().getName().startsWith(Constants.DOT))))
    } else {
      None
    }
  }

  // Get listing of all files and statistics
  def getFileStatistics(location: String): Option[Array[FileStatus]] = {
    if (isValidDirectory(location)) {
      Some(fileSystem.listStatus(getPath(location)))
    } else {
      None
    }
  }

  // Get Filesystems working directory
  def getWorkingDirectory(): String = {
    removeLastSlash(fileSystem.getWorkingDirectory().toString())
  }

  // compare 2 File Statuses
  def equalsFileStatistics(fs1: Array[FileStatus], fs2: Array[FileStatus]): Boolean = {
    if (fs1.sameElements(fs2)) {
      true
    } else {
      false
    }
  }

  // This is extremely slow
  def listAllFiles(location: String, recursive: Boolean): List[String] = {
    val lst = fileSystem.listFiles(
      getPath(location), true)
    val result = new ListBuffer[String]()
    while (lst.hasNext()) {
      result += lst.next().getPath().toString()
    }
    result.toList
  }



  // Copy a local file to HDFS
  def copyFromLocal(srcFile: String, targetFolder: String, overwriteTarget: Boolean): Unit = {
    val sourcePath = getPath(srcFile)
    val targetFolderPath = getPath(targetFolder)
    if (fileSystem.isFile(sourcePath) && fileSystem.isDirectory(targetFolderPath)) {
      fileSystem.copyFromLocalFile(false, overwriteTarget, sourcePath, targetFolderPath)
    }
  }

  // delete a single file from hdfs location
  def delete(filename: String): Unit = {
    val path = getPath(filename)
    if (fileSystem.isFile(path)) {
      fileSystem.delete(path, false)
    }
  }

  // Get File from Hadoop into an input Stream
  def getFile(filename: String): InputStream = {
    val path = getPath(filename)
    fileSystem.open(path)
  }

  // get Block Size
  def getOutputBlockSize(outputPath: Path): Long = {
    fileSystem.getDefaultBlockSize(outputPath)
  }

  // get Block Size (String)
  def getOutputBlockSize(outputPath: String): Long = {
    fileSystem.getDefaultBlockSize(getPath(outputPath))
  }

  // if the path is a valid file
  def isValidFile(location: String): Boolean = {
    val path = getPath(location)
    if (fileSystem.isFile(path)) {
      true
    } else {
      false
    }
  }

  // if the file exists
  def isValidDirectory(location: String): Boolean = {
    val path = getPath(location)
    if (fileSystem.isDirectory(path)) {
      true
    } else {
      false
    }
  }

  // if the path is already existing as file or folder
  def pathExists(location: String): Boolean = {
    if (fileSystem.isFile(getPath(location)) || fileSystem.isDirectory(getPath(location))) {
      true
    } else {
      false
    }
  }

  // If the File is an Ignore File
  def isIgnoreFile(location: Path): Boolean = {
    val strLocation = location.getName().toLowerCase()
    if (strLocation.startsWith(Constants.DOT) || strLocation.startsWith(Constants.UNDERSCORE)) {
      true
    } else {
      false
    }
  }

  // get Child folders path in parent
  def getPartitionPath(mainFolder: String, childFolder: String): String = {
    getResolvedPath(childFolder).toString().replace(getResolvedPath(mainFolder).toString(), "")
  }

  // get fully qualified path from string
  // This will work only on existing folders
  def getResolvedPath(location: String): Path = {
    fileSystem.resolvePath(new Path(location))
  }

  // get path from string
  def getPath(location: String): Path = {
    new Path(location)
  }

  // Create a new folder
  def createFolder(folderPath: String): Unit = {
    val path = getPath(folderPath)
    if (!fileSystem.exists(path)) {
      fileSystem.mkdirs(path)
    }
  }

  // Delete a folder
  def deleteFolder(folderPath: String, forced: Boolean): Unit = {
    val path = getPath(folderPath)
    if (fileSystem.exists(path) && fileSystem.isDirectory(path)) {
      fileSystem.delete(path, forced) // recursive delete can cause cluster fuck

    }
  }

}

