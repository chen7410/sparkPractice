package app

import org.apache.spark.{SparkConf, SparkContext}

object CodeShareAirline {
    private val ROUTES = "D:\\scalaworkspace\\AirlineDataset\\airlinedata\\routes.dat"
    private val AIRLINES = "D:\\scalaworkspace\\AirlineDataset\\airlinedata\\Final_airlines"
    private val ROUTE_AIRLINEID = 1
    private val AIRLINE_AIRLINEID = 0
    private val AIRLINE_AIRLINE_NAME = 1
    private val ROUTE_CODE_SHARE = 6
    private val CODE_SHARE = "Y"

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setMaster("local")
        conf.setAppName("Find nonstop airline")
        val sc = new SparkContext(conf)

        val route = sc.textFile(ROUTES)
        val airline = sc.textFile(AIRLINES)

        val routePairs = route.map(line => {
            val info = line.split(",")
            (info(ROUTE_AIRLINEID), info(ROUTE_CODE_SHARE))
        })
        val airlinePairs = airline.map(line => {
            val info = line.split(",")
            (info(AIRLINE_AIRLINEID), info(AIRLINE_AIRLINE_NAME))
        })

        val routeCodeShare = routePairs.filter(pair => pair._2 == CODE_SHARE)
        val airlineJoinRoute = airlinePairs
                .join(routeCodeShare)
                .distinct()
                .sortBy(pair => pair._1.toInt)
                .map(pair => pair._2._1)

        airlineJoinRoute.foreach(println)
    }
}
