package com.amadeus.acu.challenge.question1

import com.amadeus.acu.challenge.CommonTestSuite
import  com.amadeus.acu.challenge.question1.CSVFileHandler._

class ReadingFileTestSuite extends CommonTestSuite {

  test("read file: a right path") {
    val df = readFile("src/test/resources/test-data/sample-bookings.csv")(spark)
    assert(df.count() == 9)
  }
}
