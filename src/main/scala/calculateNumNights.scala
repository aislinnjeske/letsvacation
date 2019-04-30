import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import java.lang.Double
import org.apache.spark.sql.functions._

object calculateDailyCosts {

    def main(args: Array[String]){
    
    //Create the spark session and the spark context
    val spark = SparkSession.builder.appName("Lets-Vacation").getOrCreate()
    val sc = SparkContext.getOrCreate()
    import spark.implicits._

    //Read nightly costs data into rdd from hdfs cluster
    val nightlyCostData = sc.textFile("hdfs://honolulu:9000/sparkData/airbnbData/*.txt")
    val nightlyPairs = nightlyCostData.map(x => (x.split(",")(0).substring(1) + " " + x.split(",")(1), Double.parseDouble(x.split(",")(2).substring(0, x.split(",")(2).length()-1))))
    
    //Group costs together & filter out bad data
    val groupedNightly = nightlyPairs.groupByKey.map(x => (x._1, x._2.toList)).filter(_._2.length == 2)
    
    //Read food & transportation data into rdd from hdfs cluster
    val dailyCostsData = sc.textFile("hdfs://honolulu:9000/sparkData/foodData/*.txt")
    val dailyPairs = dailyCostsData.map(x => (x.split(",")(0).substring(1) + " " + x.split(",")(1), List(Double.parseDouble(x.split(",")(2)), Double.parseDouble(x.split(",")(3).substring(0, x.split(",")(3).length()-1)))))
    
    //Joining the two rdd's together
    val joinedData = dailyPairs.join(groupedNightly)
    val groupedData = joinedData.groupByKey()
    
    //Array[(city type, CompactBuffer(List[food, trans], List[deposit, night cost])
    val nightlyResults = groupedData.map(x => (x._1, x._2.toList)).mapValues(x => x(0)).mapValues(x => Math.floor( (2000 - x._2(1)) / (x._1(0) + x._1(1) + x._2(0)) ).toInt) 
    
    nightlyResults.take(10000).foreach(println)

    }
}
