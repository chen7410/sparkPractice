package app

import org.apache.spark.{SparkConf, SparkContext}

object AirportInCountry {
    private val PATH = "D:\\scalaworkspace\\AirlineDataset\\airlinedata\\airports_mod - Copy.csv"
    private val COUNTRY = "India"
    private val INDEX_COUNTRY = 3
    private val INDEX_AIRPORT_NAME = 1
    private val DELIMITER = " | "

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setMaster("local")
        conf.setAppName("Find airport in country")

        val sc = new SparkContext(conf)

        val lines = sc.textFile(PATH)
//        lines.collect().foreach(println)
        val pairs = lines.map(line => {
            val info = line.split(",")
            (info(INDEX_COUNTRY), info(INDEX_AIRPORT_NAME))
        })

//        val distinctKey = pairs.reduceByKey((a, b) => a + DELIMITER + b)
        val result = pairs.filter(countryAirport => countryAirport._1.toString.equals(COUNTRY))
        result.foreach(println)
        result.saveAsTextFile("D:\\scalaworkspace\\AirlineDataset\\output")
    }
}
