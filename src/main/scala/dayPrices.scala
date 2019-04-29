import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import java.lang.Double


object dayPrices {
	
	def main(args: Array[String]) {
        //Create the spark session and the spark context
        val spark = SparkSession.builder.appName("Lets-Vacation").getOrCreate()
        val sc = SparkContext.getOrCreate()
        val sqlContext = new SQLContext(sc)
        import spark.implicits._
        import sqlContext.implicits._
	
		val citiesDF = sqlContext.read.format("csv").option("header", "true").option("delimiter", ";").load("hdfs://helena:43621/cs455/term_project/city_prices.csv")
        val selectedColumns = citiesDF.select("name","currency", "1", "2", "3", "4", "6", "7", "9", "18", "19", "107", "108", "110", "113", "116").na.drop()
        val rows : RDD[Row] = selectedColumns.rdd

        // Take the average_price element of every price item
        val pricesPerItem  = rows.map(s => Array(
            s.get(0).toString,  
            s.get(2).toString.split(" ")(3), 
            s.get(3).toString.split(" ")(3), 
            s.get(4).toString.split(" ")(3), 
            s.get(5).toString.split(" ")(3), 
            s.get(6).toString.split(" ")(3),
            s.get(7).toString.split(" ")(3), 
            s.get(8).toString.split(" ")(3), 
            s.get(9).toString.split(" ")(3), 
            s.get(10).toString.split(" ")(3), 
            s.get(11).toString.split(" ")(3),
            s.get(12).toString.split(" ")(3), 
            s.get(13).toString.split(" ")(3), 
            s.get(14).toString.split(" ")(3)
        ))
        
        // Index 0 = City name
        // Index 1 = Meal, inexpensive restaurant
        // Index 2 = Meal, expensive (2 ppl)
        // Index 3 = Meal, mcdonalds
        // Index 4 = 1 Beer
        // Index 5 = Coke
        // Index 6 = Water
        // Index 7 = Bread
        // Index 8 = One-way ticket (bus)
        // Index 9 = Chicken breast
        // Index 10 = Taxi Start 
        // Index 11 = Taxi 1km
        // Index 12 = Apple
        // Index 13 = Lettuce head
      /*
        val dayPrices = pricesPerItem.map(s => (
            s(0).toString,
            s(3).toDouble + s(13).toDouble/3 + s(7).toDouble + s(9).toDouble/7 + s(12).toDouble + s(3).toDouble + s(4).toDouble,
            s(3).toDouble + s(1).toDouble + s(1).toDouble + s(4).toDouble,
            s(1).toDouble + s(1).toDouble + s(2).toDouble/2,
            s(8).toDouble*2,
            s(10).toDouble + s(11).toDouble * 10,
            s(10).toDouble + s(11).toDouble * 20
        ))
      */
      val lowPrices = pricesPerItem.map(s => (
            s(0).toString,
	        "lo",
            s(3).toDouble + s(13).toDouble/3 + s(7).toDouble + s(9).toDouble/7 + s(12).toDouble + s(3).toDouble + s(4).toDouble,
            s(8).toDouble*2
        ))


      val midPrices = pricesPerItem.map(s => (
            s(0).toString,
	        "mid",
	        s(3).toDouble + s(1).toDouble + s(1).toDouble + s(4).toDouble,
	        s(10).toDouble + s(11).toDouble * 10
        ))


      val highPrices = pricesPerItem.map(s => (
            s(0).toString,
	        "hi",
            s(1).toDouble + s(1).toDouble + s(2).toDouble/2,
            s(10).toDouble + s(11).toDouble * 20
        ))



        //dayPrices.collect

        lowPrices.take(10000).foreach(println)
        midPrices.take(10000).foreach(println)
        highPrices.take(10000).foreach(println)
	}
}
