package com.amadeus.acu.challenge.question1

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.log4j.{Level, Logger}

object CSVFileHandler {

  protected val logger: Logger = Logger.getLogger(getClass.getName)
  logger.setLevel(Level.INFO)
  /**
    * The method read a csv file into a dataFrame.
    * @param path csv file
    * @param delimiter field separator
    * @param header to specify if the csv has a header or not
    * @param spark SparkSession
    * @return a dataFrame
    */
  def readFile(path: String, delimiter: String = "^", header: String = "true")(implicit spark: SparkSession): DataFrame = {

      spark.read.format("csv").option("delimiter", delimiter).option("header", header).
        option("inferSchema", "true").load(path)
  }

  /**
    * Write down a dataFrame as csv
    * @param df dataFrame to be written
    * @param path output path
    * @param delimiter csv delimiter
    * @param header
    * @param spark a SparkSession
    */
  def writeDataFrameAsCSV(df: DataFrame, path: String, delimiter: String = "^", header: String = "true")(implicit spark: SparkSession): Unit  = {

    df.write.format("csv").option("delimiter", delimiter).option("header", header).option("compression", "bzip2").mode(SaveMode.Overwrite).save(path)

  }

  /**
    * Write dataFrame as JSON
    * @param df
    * @param path
    * @param numberOfPartition
    * @param spark
    */
  def writeDataFrameAsJSON(df: DataFrame, path: String, numberOfPartition: Int)(implicit spark: SparkSession): Unit  = {

    df.coalesce(numberOfPartition).write.mode(SaveMode.Overwrite).json(path)

  }

}
