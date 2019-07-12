package app

import org.apache.spark.{SparkConf, SparkContext}

object NonstopAirline {
    private val ROUTES = "D:\\scalaworkspace\\AirlineDataset\\airlinedata\\routes.dat"
    private val AIRLINES = "D:\\scalaworkspace\\AirlineDataset\\airlinedata\\Final_airlines"
    private val ROUTE_AIRLINEID = 1
    private val AIRLINE_AIRLINEID = 0
    private val AIRLINE_AIRLINE_NAME = 1
    private val ROUTE_STOP = 7
    private val N_STOPS = "0"


    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setMaster("local")
        conf.setAppName("Find nonstop airline")
        val sc = new SparkContext(conf)

        val route = sc.textFile(ROUTES)
        val airline = sc.textFile(AIRLINES)

        val routePairs = route.map(line => {
            val info = line.split(",")
            (info(ROUTE_AIRLINEID), info(ROUTE_STOP))
        })
        val airlinePairs = airline.map(line => {
            val info = line.split(",")
            (info(AIRLINE_AIRLINEID), info(AIRLINE_AIRLINE_NAME))
        })

        /*find the airline that has n stop*/
        val routeNonstop = routePairs.filter(pairs => pairs._2 == N_STOPS)
//        routeNonstop.foreach(println)

        val joinRouteFromAirline = routeNonstop.join(airlinePairs)
//        joinRouteFromAirline.foreach(println)

        /*eliminate the duplicated pairs and sort the pairs by their airline id*/
        val distinct = joinRouteFromAirline.distinct().sortBy(pair => pair._1.toInt)

        /*only show the airline names*/
        val result = distinct.map(pair => {
            (pair._2._2)
        })
        result.foreach(println)
    }
}
