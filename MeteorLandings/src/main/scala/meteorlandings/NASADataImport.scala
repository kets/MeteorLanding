package meteorlandings


import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.elasticsearch.spark.sql.`package`.SparkSchemaRDDFunctions
import org.elasticsearch.spark.sql._
import org.elasticsearch.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import scala.util.matching.Regex
import org.apache.spark.SparkContext
//import org.apache.spark.sql.types.StructType
//import org.apache.spark.sql.types.StringType
//import org.apache.spark.sql.types.StructField



object NASADataImport {
  //case class Person(sr: Int, ci: Int, name: String)
  def main(args: Array[String]) {
    
    //set up the Spark configuration
    val conf = new SparkConf().setAppName("LD").setMaster("spark://localhost:7077")
    conf.set("es.index.auto.create", "true");
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Create an RDD
    val nasardd = sc.textFile("src/main/resources/nasadata.csv")

    // The schema is encoded in a string
    val schemaString = "name,nametype,recclass,mass,fall,year,id,lat,lon,location"
    //Aachen,Valid,L5,21,Fell,01/01/1880 12:00:00 AM,1,50.775000,6.083330,"50.775000, 6.083330"
    // Import Spark SQL data types and Row.

    // Generate the schema based on the string of schema
    val schema =
      StructType(
        schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))

    // Convert records of the RDD (meteors) to Rows.
    val rowRDD = nasardd.map(_.split(";")).map(p => Row(p(0), p(1), p(2), p(3).toDouble, p(4), p(5).substring(6,10), p(6).toInt, p(7).toDouble, p(8).toDouble, p(9)))
    //val meteorRDD = nasardd.map(_.split(",").map(p=>MeteorLanding(p(0).toString(), p(1).toString(), p(2).toString(), p(3).toDouble, p(4).toString(), p(5).toString().substring(6,10), p(6).toInt, p(7).toDouble, p(8).toDouble, p(9).toString())))
    
    // Apply the schema to the RDD.
    val nasaSchemaRDD = sqlContext.applySchema(rowRDD, schema) //  applySchema(rowRDD, schema)
    
    nasaSchemaRDD.printSchema();
    println(nasaSchemaRDD.schema);

    // Register the SchemaRDD as a table.
    nasaSchemaRDD.registerTempTable("nasa")

    // SQL statements can be run by using the sql methods provided by sqlContext.
    val results = sqlContext.sql("SELECT * FROM nasa")

    // The results of SQL queries are SchemaRDDs and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by ordinal.
    results.map(t => "location: " + t(9)).take(25).foreach(println)
    println("Count " + results.count())
    results.saveToEs("test2/nasa2")

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