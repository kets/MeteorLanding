package meteorlandings

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.clustering.KMeans
import org.elasticsearch.spark._
import scala.collection.JavaConversions._
import org.apache.hadoop.mapred.JobConf
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.elasticsearch.spark
import org.apache.spark.serializer.KryoSerializer
import org.apache.hadoop.io.MapWritable
import org.elasticsearch.hadoop.mr.EsInputFormat
import org.apache.spark.rdd.RDD





object MeteorClusters {
  
  def main(args: Array[String]){
  
    val conf = new SparkConf
    conf.setMaster("spark://localhost:7077")
    val sc = new SparkContext(conf)    
      
    //Configure the source (index)
    val jobConf = SharedESConfig.setupEsOnSparkContext(sc, "test2/nasa2", Some("http://127.0.0.1:9200"))
    
    val query = "{\"query\": {\"filtered\" : {\"filter\" : {\"range\" : {\"year\": { \"gte\": \"1700\", \"lte\" : \"2050\" }}}}}}"
    //val query = "{\"query\": {\"filtered\" : {\"query\" : {\"match_all\" : {}}}}}"
    println("Using query "+query)
    jobConf.set("es.query", query)
      
    //val esRDD = sc.esRDD("test2/nasa2", query)
    
     // Read from ES using inputformat from org.elasticsearch.hadoop;
     // note, that key [Text] specifies the document id (_id) and
     // value [MapWritable] the document as a field -> value map
    //
    val currentResults = sc.hadoopRDD(jobConf, classOf[EsInputFormat[Object, MapWritable]], classOf[Object], classOf[MapWritable])
    println("currentResults: "+ currentResults)
    val meteors = currentResults.map{ case (key, value) => mapWritableToInput(value) }
    //val meteors = sqlCtx.esRDD("test/nasa", query)
    
    println("count: " + currentResults.count())
    // Extract the location
    val location = meteors.flatMap{meteor =>
      meteor.getOrElse("location", "").split(" ")
    }
    
   
    
 

  
  }
  
  def mapWritableToInput(in: MapWritable): Map[String, String] = {
    in.map{case (k, v) => (k.toString, v.toString)}.toMap
  }
  
  

}