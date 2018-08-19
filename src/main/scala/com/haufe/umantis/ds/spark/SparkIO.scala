/**
  * This program is free software: you can redistribute it and/or modify
  * it under the terms of the GNU General Public License as published by
  * the Free Software Foundation, either version 3 of the License, or
  * (at your option) any later version.
  *
  * This program is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  * GNU General Public License for more details.
  *
  * You should have received a copy of the GNU General Public License
  * along with this program.  If not, see <https://www.gnu.org/licenses/>.
  */

package com.haufe.umantis.ds.spark

import java.io._

import com.haufe.umantis.ds.utils.ConfigGetter
import com.typesafe.config.Config

import scala.io
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SaveMode}

case class DirectoryContent(directories: List[File], files: List[File])


trait SparkIO extends SparkSessionWrapper {

  private lazy val sparkConfig: Config = ConfigGetter.getConfig("/spark.conf")
  private lazy val sC: String = "spark-configuration"

  lazy val dataRoot: String = sparkConfig.getString(s"$sC.data-root")
  lazy val appsRoot: String = sparkConfig.getString(s"$sC.apps-root")
  lazy val kafkaParquetsDir: String = sparkConfig.getString(s"$sC.kafka-dir")
  lazy val kafkaBroker: String = sparkConfig.getString(s"$sC.kafka-broker")
  lazy val avroSchemaRegistry: String = sparkConfig.getString(s"$sC.avro-schema-registry")
  lazy val zookeeper: String = sparkConfig.getString(s"$sC.zookeeper")

  def readCSV(inFilenameLeaf: String, delimiter: String = "\t"): DataFrame = {
    val inFilename = dataRoot + inFilenameLeaf

    currentSparkSession.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", delimiter)
      .option("quote", "\"")
      .option("multiLine","true")
      .load(inFilename)
  }

  def writeCSV(df: DataFrame, outFilenameLeaf: String, delimiter: String = "\t"): Unit = {
    val outFilename = dataRoot + outFilenameLeaf + ".csv"

    df
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", "true")
      .option("delimiter", delimiter)
      .csv(outFilename)
  }

  def getDF(name: String): DataFrame = {
    readDataFrame(name)
  }

  def filePath(filenameLeaf: String, format: String = "parquet"): String = {
    dataRoot + filenameLeaf + "." + format
  }

  def readDataFrame(inFilenameLeaf: String, inFormat: String = "parquet"): DataFrame = {
    val inFilename = filePath(inFilenameLeaf, inFormat)
    currentSparkSession.read.format(inFormat).load(inFilename)
  }

  def writeDataFrame(df: DataFrame,
                     outFilenameLeaf: String,
                     outFormat: String = "parquet"): Unit = {
    val outFilename = filePath(outFilenameLeaf, outFormat)
    df.write.mode(SaveMode.Overwrite).format(outFormat).save(outFilename)
  }

  def deleteDataFrame(filenameLeaf: String, format: String = "parquet"): Unit = {
    val filename = filePath(filenameLeaf, format)
    deleteFile(filename)
  }

  def printToFile(text: String, filename: String): Unit = {
    // PrintWriter
    val pw = new PrintWriter(new File(dataRoot + filename))
    pw.write(text)
    pw.close()
  }

  def writeToFile(text: String, filename: String): Unit = {
    // FileWriter
    val file = new File(dataRoot + filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(text)
    bw.close()
  }

  def firstLine(filename: String): Option[String] = {
    val f = new File(filename)
    val src = io.Source.fromFile(f)
    try {
      src.getLines.find(_ => true)
    } finally {
      src.close()
    }
  }

  def deleteFile(outPutPath: String): Boolean = {
    val fs=FileSystem.get(currentSparkSession.sparkContext.hadoopConfiguration)
    if(fs.exists(new Path(outPutPath)))
      fs.delete(new Path(outPutPath),true)
    else
      println(s"$outPutPath file not found.")
    false
  }

  def getListOfFiles(dirLeaf: String): DirectoryContent = {
    val dir = dataRoot + dirLeaf
    val d = new File(dir)

    getListOfFiles(d)
  }

  def getListOfFiles(dir: File): DirectoryContent = {
    if (dir.exists && dir.isDirectory) {
      DirectoryContent(
        dir.listFiles.filter(_.isDirectory).toList,
        dir.listFiles.filter(_.isFile).toList)
    } else {
      DirectoryContent(List[File](), List[File]())
    }
  }

  /**
    * Spark does not allow us to read a parquet file, process it, and overwrite it so
    * we have this function that read the df, process it according to the processingFunction
    * in input, writes it to a temp parquet file, re-read it, overwrites the original one,
    * and finally delete the temp one.
    */
  def overwriteDataFrame(name: String, processingFunction: DataFrame => DataFrame): Unit = {
    val df = processingFunction(readDataFrame(name))
    writeDataFrame(df, "temp")
    val newdf = readDataFrame("temp")
    writeDataFrame(newdf, name)
    deleteFile(s"$dataRoot/temp.parquet")
  }

  def idToString(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions.{col, udf}

    val idChanger = udf({ id: Int => id.toString })
    df.withColumn("id", idChanger(col("id")))
  }
}

object IO extends SparkIO