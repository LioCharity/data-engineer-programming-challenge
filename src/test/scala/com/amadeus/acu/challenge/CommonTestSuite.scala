package com.amadeus.acu.challenge

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, DatasetSuiteBase}
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession

class CommonTestSuite extends FunSuite with BeforeAndAfterEach with   DataFrameSuiteBase{

  System.setProperty("hadoop.home.dir", "C:\\Vagrant\\winutils-master\\hadoop-2.7.1")

}
