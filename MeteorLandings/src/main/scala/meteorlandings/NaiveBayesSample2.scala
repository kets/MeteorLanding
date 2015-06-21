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
      
    
//    //val query = "{\"query\": {\"filtered\" : {\"filter\" : {\"range\" : {\"year\": { \"gte\": \"1600\", \"lte\" : \"2050\" }}}}}}"
//    val query = "{\"query\": {\"filtered\" :  {\"filter\" : {\"geo_bounding_box\" : {\"location\": { \"top_left\": { \"lat\" :  "+ 0.0 + ", \"lon\" : " + 90.0 +"  },\"bottom_right\": { \"lat\":  "+ -180.0 + ", \"lon\": " + -90.0 + "    }}}}}}}"
//    
//    //val query = "{\"query\": {\"filtered\" : {\"query\" : {\"match_all\" : {}}}}}"
    val jobConf = SharedESConfig.setupEsOnSparkContext(sc, "test3/nasa3", Some("http://127.0.0.1:9200"))
    
    // Create an RDD
    val nasardd = sc.textFile("src/main/resources/nasadata.csv")
    

    
    val rowRDD = nasardd.map(_.split(";")).map(p => Row(p(0), p(1), p(2), p(3).toDouble, p(4), p(5).substring(6,10), p(6).toInt, p(7).toDouble, p(8).toDouble, p(9)))
    println(nasardd.first())
    
    //use the hashingtf function to create vectors
    val htf = new HashingTF(500)
    
    //create labeled point
    val parsedData = nasardd.map{ row =>
      val parts = row.split(",")
    
      
    }
   
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
  
    val meteorClass = meteors.flatMap{
      meteor => meteor.getOrElse("recclass", "")
    }
    println("classCount: "+ meteorClass.countByValue())
  
      
    
  }
    
    def mapWritableToInput(in: MapWritable): Map[String, String] = {
 
    in.map{case (k, v) => (k.toString, v.toString)}.toMap
  }


}