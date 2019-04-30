import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import java.lang.Double
import org.apache.spark.sql.functions._

object readAirbnbDataDeposits {

def main(args: Array[String]){
    
    //Create the spark session and the spark context
    val spark = SparkSession.builder.appName("Lets-Vacation").getOrCreate()
    val sc = SparkContext.getOrCreate()
    val sqlContext = new SQLContext(sc)
    import spark.implicits._
    import sqlContext.implicits._
    
    //Read in the data from HDFS csv file parsing on ;
    val inputData = sqlContext.read.format("csv").option("header", "true").option("delimiter", ";").load("hdfs://carson-city:8624/termProject/cityData/*.csv")
    
    //Append the file name to the dataframe to group the housing details
    val inputDataWithFileName = inputData.withColumn("filename", input_file_name())
    
    //Select only the columns we need and drop any columns with null 
    val selectedData = inputDataWithFileName.select("filename", "country", "room_type", "security_deposit", "cleaning_fee").na.fill(Map("security_deposit" -> "$0.00", "cleaning_fee" -> "$0.00")).na.drop()
    
    val newRoomTypes = selectedData.withColumn("room_type", when(selectedData("room_type") === "Entire home/apt", "hi"). when(selectedData("room_type") === "Shared room", "lo").when(selectedData("room_type") === "Private room", "mid").otherwise(selectedData("room_type")))

    //filter out columns where the country is not a word and where the price is in $x.xx format
    val noNumberData = newRoomTypes.filter(newRoomTypes("country") rlike "^[a-zA-Z ]{3,}$")
    val noNumberData2 = noNumberData.filter(noNumberData("room_type") rlike "^[a-zA-Z/ ]{2,}$")
    val numbered = noNumberData2.filter(noNumberData2("security_deposit") rlike "^\\$[0-9]+\\.[0-9]+$")
    
    //Convert the dataframe to an rdd
    val rows : RDD[Row] = numbered.rdd
    
    //Create key value pairs with key being City, Country, Room-Type and value is the housing price
    val keyValuePairs = rows.map(s => ( s.get(0).toString.substring(s.get(0).toString.lastIndexOf('/') + 1, s.get(0).toString.indexOf('.')).capitalize.replace("-", " ") + "," + s.get(2), Double.parseDouble(s.get(3).toString.substring(1).replace(",","")) + Double.parseDouble(s.get(4).toString.substring(1).replace(",",""))))
    
    //Calculate average values for each key
    val means = keyValuePairs.groupByKey.mapValues(x => x.sum/x.size)

    //Write the means to text file
    //means.saveAsTextFile("hdfs://carson-city:8624/termProject/airbnbData/output3")
    
    //Write the means to stdout in spark
    means.take(10000).foreach(println)
    }

}
