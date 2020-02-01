package com.amadeus.acu.challenge

import org.apache.spark.sql.SparkSession
import  com.amadeus.acu.challenge.question1.CSVFileHandler._
import com.amadeus.acu.challenge.question2.BusiestAirports._
import com.amadeus.acu.challenge.question3.SearchesAndBookings._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._

object ProgrammingChallengeEntry {

  protected val logger: Logger = Logger.getLogger(getClass.getName)
  logger.setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = SparkSession.builder().appName("DE ACU programming challenge").getOrCreate()
    import spark.implicits._

    logger.info("Main entry point")

    logger.info("Parse command line arguments")
    val commandLineArguments =  ArgumentParser.init().parse(args, ArgumentParser.Arguments())
    val bookingsPath = commandLineArguments.get.bookings
    val searchesPath = commandLineArguments.get.searches
    val outputDirectory = commandLineArguments.get.output
    val refAirportCity = commandLineArguments.get.refAirportPopularity

    try {

      logger.info("Start question 1: File reading")
      val bookingsDF = readFile(bookingsPath)
      val searchesDF = readFile(searchesPath)

      logger.info("display dataFrames: bookings and searches")
      logger.info("Persist dataFrames because they will be reused many times")
      bookingsDF.persist(StorageLevel.MEMORY_AND_DISK)
      searchesDF.persist(StorageLevel.MEMORY_AND_DISK)

      println("*** Number of bookings: " + bookingsDF.count())
      println("*** Number of searches: " + searchesDF.count())
      println("*** Total number of passengers: " + bookingsDF.agg(sum($"pax")).show())

      logger.info("Start question 2: The busiest airport")
      var topAirportsDF = busiestAirports(bookingsDF)
      topAirportsDF = fromAirportCodeToCityName(topAirportsDF, refAirportCity)
      println("Top 10 airports")
      topAirportsDF.select($"airport_code", $"numberOfPassengers", $"city").show(10)
      writeDataFrameAsJSON(topAirportsDF, outputDirectory + "/question2", 1)

      logger.info("Start question 3: Converted searches")
      val mappedSearchedAndBookingsDF = mapSearchesWithBookings(searchesDF, bookingsDF)
      writeDataFrameAsCSV(mappedSearchedAndBookingsDF, outputDirectory + "/question3")

    }finally {
      spark.stop()
    }

  }
}