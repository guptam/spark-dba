
/*
* Author: Manish Gupta
*/

package com.guptam.spark.dba.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.avro.Schema

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.fs.{ FileStatus, Path }
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.avro.generic.{ GenericDatumReader, GenericRecord }
import org.apache.avro.mapred.{ AvroKey, FsInput }
import org.apache.avro.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import scala.collection.mutable.ListBuffer

import org.apache.avro.mapred.FsInput
import com.guptam.spark.dba.common._
import com.guptam.spark.dba.hdfsutil.HDFSAvroOperations
import org.apache.avro.mapred.AvroMultipleInputs

// scalastyle:off println
object TemplateApp {
  def main(args: Array[String]): Unit = {

    val hdfs = new HDFSAvroOperations("")
    val mrJob = new Job()

    FileInputFormat.setInputPaths(mrJob, "D:/code/spark-dba/data/AvroFolderCompactor/sourcePartitioned/hello/avrodata/")
    FileInputFormat.setInputDirRecursive(mrJob, true)

    val schema = new Schema.Parser().parse(hdfs.getFile("D:/code/spark-dba/data/AvroFolderCompactor/schema/hello.avsc"))
    // if Schema is available, then use it
    AvroJob.setInputKeySchema(mrJob, schema)
    AvroJob.setOutputKeySchema(mrJob, schema)

    FileOutputFormat.setOutputPath(mrJob, hdfs.getPath("D:/code/spark-dba/data/AvroFolderCompactor/target/hello/avrodata"))
    FileOutputFormat.setCompressOutput(mrJob, true)
    FileOutputFormat.setOutputCompressorClass(
      mrJob, classOf[org.apache.hadoop.io.compress.SnappyCodec])
    mrJob.setOutputFormatClass(classOf[AvroKeyOutputFormat[GenericRecord]])

    val conf = new SparkConf().setAppName("TemplateApp").setMaster("local")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkContext = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)

    val avroRDD = sparkContext.newAPIHadoopRDD(
            mrJob.getConfiguration(),
            classOf[AvroKeyInputFormat[GenericRecord]],
            classOf[AvroKey[GenericRecord]],
            classOf[NullWritable])
            .cache


    val j = sqlContext.read.json(avroRDD.map(f => f._1.datum().toString()))
    j.printSchema()
    println(j.count())


    sparkContext.stop()
  }
}



