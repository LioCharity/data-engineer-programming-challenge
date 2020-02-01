package com.amadeus.acu.challenge.question2

import com.amadeus.acu.challenge.CommonTestSuite
import com.amadeus.acu.challenge.question2.BusiestAirports._
import com.amadeus.acu.challenge.question1.CSVFileHandler._

class BusiestAirportTestSuite extends CommonTestSuite {

  test("ranked busiest airports"){
    var bookingsDF = readFile("src/test/resources/test-data/sample-bookings.csv")(spark)
    bookingsDF = busiestAirports(bookingsDF)(spark)
    bookingsDF.show()
    val expectedResult = Array(
      Array("SIN", 4, 2),
      Array("SVO", 2, 2),
      Array("LGA", 2, 2),
      Array("CLT", 2, 2),
      Array("LHR", -1, 1)
    )

    val result = bookingsDF.collect().map(_.toSeq)

    assert(expectedResult === result)

    // airport code to city conversion
    val convertedDF = fromAirportCodeToCityName(bookingsDF, "src/main/resources/ref_airport_popularity.csv")(spark)
    val expectedCityConversion = Array(
      Array("sin", "SIN", 4, 2, "singapore"),
      Array("svo", "SVO", 2, 2, "moscow"),
      Array("lga", "LGA", 2, 2, "new york ny"),
      Array("clt", "CLT", 2, 2, "charlotte nc"),
      Array("lhr", "LHR", -1, 1, "london")
    )
    assert(expectedCityConversion === convertedDF.collect().map(_.toSeq))
  }
}
