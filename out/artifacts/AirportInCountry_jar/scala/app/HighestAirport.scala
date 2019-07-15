package app

import org.apache.spark.{SparkConf, SparkContext}

object HighestAirport {
    private val PATH = "D:\\scalaworkspace\\AirlineDataset\\airlinedata\\airports_mod - Copy.csv"
    private val INDEX_AIRPORT_HEIGHT = 8
    private val INDEX_COUNTRY = 3


    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setMaster("local")
        conf.setAppName("Which country has the highest airport")
        val sc = new SparkContext(conf)

        val airport = sc.textFile(PATH)
        val airportPairs = airport.map(line => {
            val info = line.split(",")
            (info(INDEX_COUNTRY), info(INDEX_AIRPORT_HEIGHT).toFloat)
        })

        val max = airportPairs.takeOrdered(1)(Ordering[Float].reverse.on(x => x._2))
        max.foreach(println)
    }
}
