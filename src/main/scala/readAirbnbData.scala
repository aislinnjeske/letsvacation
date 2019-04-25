import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import java.lang.Double

object readAirbnbData {

def main(args: Array[String]){
    
    //Create the spark session and the spark context
    val spark = SparkSession.builder.appName("Lets-Vacation").getOrCreate()
    val sc = SparkContext.getOrCreate()
    val sqlContext = new SQLContext(sc)
    import spark.implicits._
    import sqlContext.implicits._
    
    //Read in the data from HDFS csv file parsing on ;
    val inputData = sqlContext.read.format("csv").option("header", "true").option("delimiter", ";").load("hdfs://carson-city:8624/termProject/airbnbData/airbnb-listings.csv")
    
    //Select only the columns we need and drop any columns with null 
    //val selectedData = inputData.select("City", "Country", "Room Type", "Price", "Security Deposit", "Cleaning Fee", "Accomodates").na.drop()
    val selectedData = inputData.select("City", "Country", "Room Type", "Price").na.drop()
    
    //filter out columns where the city is not a word
    val noNumberData = selectedData.filter(selectedData("City") rlike "^[a-zA-Z ]+$")
    val numberedPrice = noNumberData.filter(noNumberData("Price") rlike "^[0-9]+$")
    
    //Convert the dataframe to an rdd
    val rows : RDD[Row] = numberedPrice.rdd
    
    //Create key value pairs with key being City, Country, Room Type
    //val keyValuePairs = rows.map(s => (s.get(0).toString.toUpperCase + "," + s.get(1) + "," + s.get(2), Double.parseDouble(s.get(3).toString)))
    val keyValuePairs = rows.map(s => (s.get(1).toString.toUpperCase + "," + s.get(2), Double.parseDouble(s.get(3).toString)))
    
    //Calculate average values for each key
    val means = keyValuePairs.groupByKey.mapValues(x => x.sum/x.size)
    
    //Write the means to text file
    //means.saveAsTextFile("hdfs://carson-city:8624/termProject/airbnbData/output2")
    
    means.take(10000).foreach(println)
    }

}
