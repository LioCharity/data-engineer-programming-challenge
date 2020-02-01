package com.amadeus.acu.challenge.question3

import com.amadeus.acu.challenge.CommonTestSuite
import com.amadeus.acu.challenge.question1.CSVFileHandler._
import com.amadeus.acu.challenge.question3.SearchesAndBookings._
import org.apache.spark.sql.functions._
import  com.amadeus.acu.challenge.Constants.IS_THE_SEARCH_BOOKED

class SearchesAndBookingsTestSuite extends CommonTestSuite{

  test("Searches ended up in bookings"){

    val bookingsDF = readFile("src/test/resources/test-data/sample-bookings.csv")(spark)
    val searchesDF = readFile("src/test/resources/test-data/sample-searches.csv")(spark)
    val resultDF = mapSearchesWithBookings(searchesDF, bookingsDF)(spark)
    val expectedResult = Array(Array(0),Array(0),Array(0),Array(0),Array(0),Array(0),Array(0),Array(0),Array(1),Array(1))
    val result = resultDF.select(col(IS_THE_SEARCH_BOOKED)).collect().map(_.toSeq)
    assert(expectedResult === result)

    resultDF.printSchema()
    resultDF.show()
    writeDataFrameAsCSV(resultDF, "src/test/resources/output/csv", delimiter = ",")(spark)
    writeDataFrameAsJSON(resultDF, "src/test/resources/output/json", 1)(spark)
  }
}
