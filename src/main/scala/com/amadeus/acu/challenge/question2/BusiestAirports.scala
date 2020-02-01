package com.amadeus.acu.challenge.question2

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import com.amadeus.acu.challenge.question1.CSVFileHandler._

object BusiestAirports {

  /**
    * The method receives a dataFrame of bookings, aggregate the number of passenger per airport
    * @param bookingsDF
    * @param spark
    * @return DataFrame with the schema (arr_port, numberOfPassengers, numberOfAirports)
    */
  def busiestAirports(bookingsDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val df = bookingsDF.
      select(col("arr_port"), col("pax")).
      groupBy("arr_port").
      agg(
        sum(col("pax")).alias("numberOfPassengers"),
        count(col("arr_port")).alias("numberOfAirports")
      ).orderBy(col("numberOfPassengers").desc)

    // remove white spaces from the strings in the column arr_port
    df.withColumn("arr_port", trim(col("arr_port")))
  }

  /**
    * Increment the input dataFrame with a column containing the full city name
    * @param df dataFrame with the column arr_port containing the iata codes of airports
    * @param spark a SparkSession
    * @return DataFrame
    */
  def fromAirportCodeToCityName(df: DataFrame, refAirportCityFile: String)(implicit spark: SparkSession): DataFrame = {
    val refAirportPopularity = readFile(refAirportCityFile).
      select(col("city"), col("airport_code"))

    df.withColumn("airport_code", lower(col("arr_port"))).
      join(refAirportPopularity, Seq("airport_code"), "leftouter")
  }
}
