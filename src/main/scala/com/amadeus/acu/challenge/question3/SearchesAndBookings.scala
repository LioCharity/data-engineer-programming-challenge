package com.amadeus.acu.challenge.question3

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.amadeus.acu.challenge.Constants._
import org.apache.log4j.{Level, Logger}

object SearchesAndBookings {

  protected val logger: Logger = Logger.getLogger(getClass.getName)
  logger.setLevel(Level.INFO)

  /**
    * The method returns the search dataFrame incremented with an additional column saying with 0(NO) and
    * 1(YES) if the search ended up in a booking.
    * @param searchesDF
    * @param bookingsDF
    * @param spark
    * @return DataFrame
    */
  def mapSearchesWithBookings(searchesDF: DataFrame, bookingsDF: DataFrame)(implicit spark: SparkSession): DataFrame = {

    // Trim white spaces from the columns that will be used for the join
    val trimmedSearchesDF = searchesDF.
      withColumn("Origin", trim(col("Origin"))).
      withColumn("Destination", trim(col("Destination")))
    val trimmedBookingsDF = bookingsDF.
      withColumn("dep_port", trim(col("dep_port"))).
      withColumn("arr_port", trim(col("arr_port")))

    // Join searches and bookings dataFrame on the origin and destination columns
    // The proper way of doing this join would've been to have a unique identifier in the searches and in the bookings
    var mappedDataDF = trimmedSearchesDF.
      join(trimmedBookingsDF, col("Origin") === col("dep_port") && col("Destination") === col("arr_port"), "leftouter")

    // Add the additional column that specifies if the search ended up in a booking
    def isSearchBookedUDF = udf((dep_port: String)=>{
      if(dep_port == null) 0 else 1
    })
    mappedDataDF = mappedDataDF.withColumn(IS_THE_SEARCH_BOOKED, isSearchBookedUDF(col("dep_port")))

    // select the searches columns plus the additional one(IS_THE_SEARCH_BOOKED): I do not remove the duplicates
    var searchesColumns: Array[String] = searchesDF.columns
    searchesColumns +:= IS_THE_SEARCH_BOOKED
    mappedDataDF.select(searchesColumns.map(col): _*)
  }
}
