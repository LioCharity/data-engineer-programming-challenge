package com.amadeus.acu.challenge

import scopt.OptionParser

object ArgumentParser {

  case class Arguments(
                        var bookings: String = "",
                        var searches: String = "",
                        var output: String = "",
                        var refAirportPopularity: String = ""
                      )

  def init(): OptionParser[Arguments] = {

    new OptionParser[Arguments]("Arguments") {

      head("Command Line Argument Parser with scopt: ACU Programming Challenge")

      override def errorOnUnknownArgument = false
      opt[String]('o', "output").required().action((x, c) =>
        c.copy( output = x) ).text("output directory path")

      opt[String]('b', "bookings").required().action((x, c) =>
        c.copy( bookings = x) ).text("bookings")

      opt[String]('s', "searches").required().action((x, c) =>
        c.copy( searches = x) ).text("searches")

      opt[String]('r', "refAirportPopularity").required().action((x, c) =>
        c.copy( refAirportPopularity = x) ).text("path to the file used to convert iata airport code to city name")
    }
  }
}
