package meteorlandings


import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.elasticsearch.spark.sql._
import org.elasticsearch.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import scala.util.matching.Regex
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext    
import org.apache.spark.sql.SQLContext._
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.functions._

object NASADataImport {
  
  
  def main(args: Array[String]) {
    
    //set up the Spark configuration
    val conf = new SparkConf().setAppName("NASAImport").setMaster("spark://localhost:7077")
    conf.set("es.index.auto.create", "true");
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sc.addJar("target/scala-2.10/meteors-landings_2.10-1.0.jar")
//    sc.addJar("lib/elasticsearch-2.1.1.jar")
//    sc.addJar("lib/elasticsearch-spark_2.11-2.2.0-rc1.jar")
//    sc.addJar("lib/spark-assembly-1.6.0-hadoop2.6.0.jar")
    import sqlContext.implicits._

    // Create an RDD, then convert to DataFrame --> update from Spark 1.2 to Spark 1.6 which introduces dataframes
    val nasardd = sc.textFile("src/main/resources/nasadata.csv")
                    .map(_.split(";"))
                    .map(p => MeteorLanding(p(0), p(1), p(2), p(3).toDouble, p(4), p(5).substring(6,10), p(6).toInt, p(7).toDouble, p(8).toDouble, p(9))).toDF()
    
                    
    nasardd.registerTempTable("nasa") 

//    // The schema is encoded in a string
//    val schemaString = "name,nametype,recclass,mass,fall,year,id,lat,lon,location"
//    //Aachen,Valid,L5,21,Fell,01/01/1880 12:00:00 AM,1,50.775000,6.083330,"50.775000, 6.083330"
//    // Import Spark SQL data types and Row.
//
//    // Generate the schema based on the string of schema
//    //val schema =  schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))
//
//    // Convert records of the RDD (meteors) to Rows.
//    val rowRDD = nasardd.map(_.split(";")).map(p => Row(p(0), p(1), p(2), p(3).toDouble, p(4), p(5).substring(6,10), p(6).toInt, p(7).toDouble, p(8).toDouble, p(9)))
//       
//    // Apply the schema to the RDD.
//    val nasaSchemaRDD = sqlContext.applySchema(rowRDD, schema) //  applySchema(rowRDD, schema)
//    
//    nasaSchemaRDD.printSchema();
    println(nasardd.schema);

    // Register the SchemaRDD as a table.
    //nasaSchemaRDD.registerTempTable("nasa")

    // SQL statements can be run by using the sql methods provided by sqlContext.
    val results = sqlContext.sql("SELECT * FROM nasa")

    // The results of SQL queries are SchemaRDDs and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by ordinal.
    results.map(t => "location: " + t(9)).take(400).foreach(println)
    println("Count " + results.count())
    results.saveToEs("test3/nasa3")

  } 
  

}

case class MeteorLanding(
  name: String,
  nametype: String, 
  recclass: String, 
  massFF: Double,
  fall: String,
  year: String,
  id : Int,
  lat : Double,
  lon : Double,
  location : String
  ){
}