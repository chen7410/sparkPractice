package app


import org.apache.spark.{SparkConf, SparkContext}

object ActiveAirlineInCountry {
    private val AIRLINES = "D:\\scalaworkspace\\AirlineDataset\\airlinedata\\Final_airlines"
    private val INDEX_ACTIVE = 7
    private val INDEX_COUNTRY = 6
    private val INDEX_IATA = 3
    private val INDEX_AIRLINE_NAME = 1
    private val COUNTRY = "united states"
    private val ACTIVE = "Y"

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setMaster("local")
        conf.setAppName("Active airline")
        val sc = new SparkContext(conf)

        val airline = sc.textFile(AIRLINES)
        val pairs = airline.map(line => {
            val info = line.split(",")
            (info(INDEX_COUNTRY), Array(info(INDEX_AIRLINE_NAME), info(INDEX_IATA), info(INDEX_ACTIVE)))
        })

        val filterPairs = pairs.filter(pair => pair._1.equalsIgnoreCase(COUNTRY)
            && pair._2(2).equalsIgnoreCase(ACTIVE) && pair._2(1)
            .matches("[a-zA-Z]{2}"))
            .distinct().sortBy(x => x._2(0))

        filterPairs.foreach(pair => {
            println(pair._1 + "," + pair._2.mkString(","))
        })
    }

}
