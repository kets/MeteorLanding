package meteorlandings

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


object MeteorsByYear {
   
  def main(args: Array[String]){
    val conf = new SparkConf
    conf.setMaster("spark://localhost:7077")
    val sc = new SparkContext(conf)    
    
     //Configure the source (index)
     val jobConf = SharedESConfig.setupEsOnSparkContext(sc, "test/nasa", Some("http://127.0.0.1:9200"))
     
    
    //Setup the query
    val query = "{\"query\": {\"filtered\" : {\"filter\" : {\"range\" : {\"year\": { \"gte\": \"1700\", \"lte\" : \"2050\" }}}}}}"
     //val query = "{\"query\": {\"filtered\" : {\"query\" : {\"match_all\" : {}}}}}"
    println("Using query "+query)
    jobConf.set("es.query", query)
    
   
    //Create a Spark RDD on top of Elasticsearch through EsInputFormat
    val currentResults = sc.hadoopRDD(jobConf, classOf[EsInputFormat[Object, MapWritable]], classOf[Object], classOf[MapWritable])
    println("currentResults: "+ currentResults)
    val meteors = currentResults.map{ case (key, value) => mapWritableToInput(value) }
    //val meteors = sqlCtx.esRDD("test/nasa", query)
    
    println("count: " + currentResults.count())
    // Extract the year
    val year = meteors.flatMap{meteor =>
      meteor.getOrElse("year", "").split(" ")
    }
    
    val meteorClass = meteors.flatMap{
      meteor => meteor.getOrElse("recclass", "").split("")
    }
    
    val yearCounts = year.countByValue()
    println("yearCounts: "+ yearCounts)
    
    //val meteorClass = meteorClass.countByValue()
    
      
  }
  
  def mapWritableToInput(in: MapWritable): Map[String, String] = {
    in.map{case (k, v) => (k.toString, v.toString)}.toMap
  }
   
  
}

