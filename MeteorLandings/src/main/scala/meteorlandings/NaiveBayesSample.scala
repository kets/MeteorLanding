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


object NaiveBayesSample {
  
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
    
    val esRDD = sc.esRDD("test2/nasa2", query)
    
     // Read from ES using inputformat from org.elasticsearch.hadoop;
     // note, that key [Object] specifies the document id (_id) and
     // value [MapWritable] the document as a field -> value map (location -> "34.45,23.45"
    //
    val currentResults = sc.hadoopRDD(jobConf, classOf[EsInputFormat[Object, MapWritable]], classOf[Object], classOf[MapWritable])
    println("currentResults: "+ currentResults)
    val meteors = currentResults.map { 
      case (key, value) => mapWritableToInput(value) 
      
    }
    //val meteors = sqlCtx.esRDD("test/nasa", query)
       
    println("count: " + currentResults.count())
    
    val meteorsMap = meteors.zipWithIndex().collect().toMap
    //meteorsMap.foreach({case (key,value) => println(">>> key=" + key.getOrElse("year", "") + ", value=" + value)})
    
    val fields = new Array[String](97)
   // val parsedData = meteorsMap.foreach({case (key,value) => LabeledPoint(key.getOrElse("year", "").toDouble, toVector(key.getOrElse("location", "").split(","), fields))})
    
//    val parsedData = meteorsMap.map{line => 
//      val year = line._1.getOrElse("year", "").toDouble
//      
//      LabeledPoint(year, Vectors.dense(line._1.getOrElse("location", "").split(",").map(_.toDouble)))}
    
    
    val parsedData = meteors.flatMap{line => 
      val year = line.getOrElse("year", "").toDouble
      
      LabeledPoint(year, Vectors.dense(line.getOrElse("location", "").split(",").map(_.toDouble), line.getOrElse("mass", "").toDouble))
      
    }
    
    //println(meteorsMap)
    //val vectors = meteors.map(meteor => toVector(meteor.getOrElse("location", "").split(","), fields))
   
    
   // val model = NaiveBayes.train(parsedData)
    
    // Split data into training (60%) and test (40%).
    val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0)
    val test = splits(1)

    
    
    
    // Extract the geelocation from each document and create a vector
   
    
  }
    private def toVector(data:Array[String], fields:Array[String]):Vector = {

    val lat = data(0).toDouble
    val lon = data(1).toDouble

    Vectors.dense(Array(lat,lon))

  }
  
  
  
  def mapWritableToInput(in: MapWritable): Map[String, String] = {
    in.map{case (k, v) => (k.toString, v.toString)}.toMap
  }
  

}