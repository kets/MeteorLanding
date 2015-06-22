package meteorlandings

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression.LabeledPoint
import org.elasticsearch.spark._
import org.apache.hadoop.io.MapWritable
import org.elasticsearch.hadoop.mr.EsInputFormat
import org.apache.spark.rdd.RDD
import org.elasticsearch.spark
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.sql._
import scala.collection.JavaConversions._
import org.apache.hadoop.mapred.JobConf
import org.elasticsearch.common.geo.GeoPoint
import org.apache.spark.mllib.feature._

object NaiveBayesSample2 {
  
   def main(args: Array[String]){
    val conf = new SparkConf
    conf.setMaster("spark://localhost:7077")
    conf.setAppName("MeteorLandings")
    val sc = new SparkContext(conf)   
    val sqlContext = new SQLContext(sc)
      
    
//    //val query = "{\"query\": {\"filtered\" : {\"filter\" : {\"range\" : {\"year\": { \"gte\": \"1600\", \"lte\" : \"2050\" }}}}}}"
//    val query = "{\"query\": {\"filtered\" :  {\"filter\" : {\"geo_bounding_box\" : {\"location\": { \"top_left\": { \"lat\" :  "+ 0.0 + ", \"lon\" : " + 90.0 +"  },\"bottom_right\": { \"lat\":  "+ -180.0 + ", \"lon\": " + -90.0 + "    }}}}}}}"
//    
//    //val query = "{\"query\": {\"filtered\" : {\"query\" : {\"match_all\" : {}}}}}"
    val jobConf = SharedESConfig.setupEsOnSparkContext(sc, "test3/nasa3", Some("http://127.0.0.1:9200"))
    
    // Create an RDD
    val nasardd = sc.textFile("src/main/resources/nasadata.csv")
    
     // The schema is encoded in a string
    val schemaString = "name,nametype,recclass,mass,fall,year,id,lat,lon,location"
    
    // Generate the schema based on the string of schema
    val schema =
      StructType(
        schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))

    
    val rowRDD = nasardd.map(_.split(";")).map(p => Row(p(0), p(1), p(2), p(3).toDouble, p(4), p(5).substring(6,10), p(6).toInt, p(7).toDouble, p(8).toDouble, p(9)))
    
    // Apply the schema to the RDD.
    val nasaSchemaRDD = sqlContext.applySchema(rowRDD, schema) //  applySchema(rowRDD, schema)
    
    // Register the SchemaRDD as a table.
    nasaSchemaRDD.registerTempTable("nasa")

    
    val recclassRDD = sc.textFile("src/main/resources/recclass-type.txt")
    
    val typeSchemaString = "type,id"
    
    val typeSchema = StructType(typeSchemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    
    val typeRowRdd = recclassRDD.map(_.split(",")).map(p => Row(p(0), p(1)))
    
    val typeSchemaRDD = sqlContext.applySchema(typeRowRdd, typeSchema)
    
    typeSchemaRDD.registerTempTable("type_table")
    
    
    val joinResults = sqlContext.sql("SELECT * FROM nasa, type_table where nasa.recclass = type_table.type").collect()
    
//    joinResults.map ( x => x(0) + " " + x(1) + " " + x(2) +" "+x(3) + " " +x(4) + " " + x(5) + " " + x(6) +" "+x(7) + " " 
//        + x(8) + " " + x(9) + " " + x(10) +" "+x(11)).take(5).foreach(println)
    
    val resultsRDD = sc.parallelize(joinResults)
    
    //use the hashingtf function to create vectors
    val htf = new HashingTF(500)
    
    val labeledRdd = resultsRDD.map { x => 
      LabeledPoint(x.getString(11).toDouble, htf.transform(x))      
    }
    
  // Split data into training (60%) and test (40%).
      val splits = labeledRdd.randomSplit(Array(0.7, 0.3), seed = 11L)
      val training = splits(0)
       println("training: "+ training.count())
      val test = splits(1)
      println("test: "+ test.count())
      val model = NaiveBayes.train(training, lambda = 1.0)
       println("model: "+ model.labels.length)
      
      val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
      println("----predictionAndLabel-----"+ predictionAndLabel.count())
      val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
       println("accuracy: " + accuracy)
       createIndexForLabel(sc, jobConf)
   
    
  }
     private def createIndexForLabel(sc : SparkContext, jobConf : JobConf) {
    //get a list of all the recclasses and assign a number to them
    //Setup the query
    val query = "{\"query\": {\"filtered\" : {\"filter\" : {\"range\" : {\"year\": { \"gte\": \"1700\", \"lte\" : \"2050\" }}}}}}"
     //val query = "{\"query\": {\"filtered\" : {\"query\" : {\"match_all\" : {}}}}}"
    println("Using query "+query)
    jobConf.set("es.query", query)
    sc.hadoopRDD(jobConf, classOf[EsInputFormat[Object, MapWritable]], classOf[Object], classOf[MapWritable])
  
    val currentResults = sc.hadoopRDD(jobConf, classOf[EsInputFormat[Object, MapWritable]], classOf[Object], classOf[MapWritable])
    val meteors = currentResults.map{ case (key, value) => mapWritableToInput(value) }
    println(meteors.first())
  
    val meteorClass = meteors.map{
      meteor => meteor.getOrElse("recclass", "")
    }
    println("classCount: "+ meteorClass.countByValue())
  
      
    
  }
    

  
    
    def mapWritableToInput(in: MapWritable): Map[String, String] = {
 
    in.map{case (k, v) => (k.toString, v.toString)}.toMap
  }


}