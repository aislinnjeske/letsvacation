import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import java.lang.Double
import org.apache.spark.sql.functions.input_file_name

object readAirbnbData {

def main(args: Array[String]){
    
    //Create the spark session and the spark context
    val spark = SparkSession.builder.appName("Lets-Vacation").getOrCreate()
    val sc = SparkContext.getOrCreate()
    val sqlContext = new SQLContext(sc)
    import spark.implicits._
    import sqlContext.implicits._
    
    //Read in the data from HDFS csv file parsing on ;
    val inputData = sqlContext.read.format("csv").option("header", "true").option("delimiter", ";").load("hdfs://carson-city:8624/termProject/cityData/*.csv")
    val inputDataWithFileName = inputData.withColumn("filename", input_file_name())
    
    //Select only the columns we need and drop any columns with null 
    //val selectedData = inputData.select("City", "Country", "Room Type", "Price", "Security Deposit", "Cleaning Fee", "Accomodates").na.drop()
    val selectedDataPrice = inputDataWithFileName.select("filename", "country", "room_type", "price").na.drop()
    //val selectedDataFee = inputData.select("market", "country", "room_type", "security_deposit", "cleaning_fee").na.drop()

    //filter out columns where the city is not a word
    val noNumberDataPrice = selectedDataPrice.filter(selectedDataPrice("country") rlike "^[a-zA-Z ]{2,}$")
    val numberedPrice = noNumberDataPrice.filter(noNumberDataPrice("price") rlike "^\\$[0-9]+\\.[0-9]+$")
    
    //val noNumberDataFee = selectedDataFee.filter(selectedDataFee("room_type") rlike "^[a-zA-Z]+$")
    //val numberedFee = noNumberDataFee.filter(noNumberDataFee("security_deposit") rlike "^\\$[0-9]+\\.[0-9]+$")
    
    //Convert the dataframe to an rdd
    val rowsPrice : RDD[Row] = numberedPrice.rdd
    //val rowsFee : RDD[Row] = numberedFee.rdd
    
    //Create key value pairs with key being City, Country, Room Type
    //val keyValuePairs = rows.map(s => (s.get(0).toString.toUpperCase + "," + s.get(1) + "," + s.get(2), Double.parseDouble(s.get(3).toString)))
    val keyValuePairsPrice = rowsPrice.map(s => ( s.get(0).toString.substring(s.get(0).toString.lastIndexOf('/') + 1, s.get(0).toString.indexOf('.')) + "," + s.get(1) + "," + s.get(2), Double.parseDouble(s.get(3).toString.substring(1).replace(",",""))))
    //val keyValuePairsFee = rowsFee.map(s => (s.get(0).toString + "," + s.get(1) + "," + s.get(2), 
    //Double.parseDouble(s.get(3).toString.substring(1).replace(",","")) + Double.parseDouble(s.get(4).toString.substring(1).replace(",",""))))
    
    //Calculate average values for each key
    val meansPrice = keyValuePairsPrice.groupByKey.mapValues(x => x.sum/x.size)
    //val meansFee = keyValuePairsFee.groupByKey.mapValues(x => x.sum/x.size)

    //Write the means to text file
    //means.saveAsTextFile("hdfs://carson-city:8624/termProject/airbnbData/output3")
    
    meansPrice.take(10000).foreach(println)
    //meansFee.take(10).foreach(println)
    }

}
