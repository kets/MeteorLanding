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


object NaiveBayesSample {
  
  def main(args: Array[String]){
    val conf = new SparkConf
    conf.setMaster("spark://localhost:7077")
    val sc = new SparkContext(conf)    
      
    
    val query = "{\"query\": {\"filtered\" : {\"filter\" : {\"range\" : {\"year\": { \"gte\": \"1600\", \"lte\" : \"2050\" }}}}}}"
    
    //val query = "{\"query\": {\"filtered\" : {\"query\" : {\"match_all\" : {}}}}}"
    val jobConf = SharedESConfig.setupEsOnSparkContext(sc, "test3/nasa3", Some("http://127.0.0.1:9200"))
    
    val esRDD = sc.esRDD("test3/nasa3", query)
    
     // Read from ES using inputformat from org.elasticsearch.hadoop;
     // note, that key [Object] specifies the document id (_id) and
     // value [MapWritable] the document as a field -> value map (location -> "34.45,23.45"
    //
//    val currentResults = sc.hadoopRDD(jobConf, classOf[EsInputFormat[Object, MapWritable]], classOf[Object], classOf[MapWritable])
//    println("currentResults: "+ currentResults)
//    val meteors = currentResults.map { 
//      case (key, value) => mapWritableToInput(value) 
//      
//    }
//    //val meteors = sqlCtx.esRDD("test/nasa", query)
//       
//    println("count: " + currentResults.count())
//    
//    val meteorsMap = meteors.zipWithIndex().collect().toMap
//    //meteorsMap.foreach({case (key,value) => println(">>> key=" + key.getOrElse("year", "") + ", value=" + value)})
//    
//    val fields = new Array[String](97)
   // val parsedData = meteorsMap.foreach({case (key,value) => LabeledPoint(key.getOrElse("year", "").toDouble, toVector(key.getOrElse("location", "").split(","), fields))})
    
//    val parsedData = meteorsMap.map{line => 
//      val year = line._1.getOrElse("year", "").toDouble
//      
//      LabeledPoint(year, Vectors.dense(line._1.getOrElse("location", "").split(",").map(_.toDouble)))}
    
    
//    val parsedData = meteors.flatMap{line => 
//      val year = line.getOrElse("year", "").split("")     
//      
//      //val year = line.getOrElse("year", "").toDouble
//      
//      //LabeledPoint(year, Vectors.dense(line.getOrElse("location", "").split(",").map(x => x.toDouble), line.getOrElse("mass", "").toDouble))
//     
//     
//    }
    
//    val year = meteors.flatMap{meteor =>
//      meteor.getOrElse("year", "").split(" ")
//    }
//    val yearCounts = year.countByValue()
//    
//    val parsedData = yearCounts.map { line =>
//      val year = line._1.toDouble
//      val counts = line._2.toDouble
//       LabeledPoint(year, Vectors.dense(counts.toDouble))
//    }
//    
//   
//    println(parsedData)    
    //println(meteorsMap)
    //val vectors = meteors.map(meteor => toVector(meteor.getOrElse("location", "").split(","), fields))
   
    
    //val model = NaiveBayes.train(parsedData)
//    
////    // Split data into training (60%) and test (40%).
//    val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
//    val training = splits(0)
//    val test = splits(1)

    
    
    
    // Extract the geelocation from each document and create a vector
    
    
    
    
    val regionImpacts =  mapRegionsToCoordinates().map{ line =>
      getImpactsByRegion(sc, jobConf, line)
      }
   
    
  }
  
  
  
  private def toVector(data:Array[String], fields:Array[String]):Vector = {

    val lat = data(0).toDouble
    val lon = data(1).toDouble
  
    Vectors.dense(Array(lat,lon))

  }
    
  def mapWritableToInput(in: MapWritable): Map[String, String] = {
    in.map{case (k, v) => (k.toString, v.toString)}.toMap
  }
  
  def getImpactsByRegion(sc : SparkContext, jobConf : JobConf, regionGeo : (Double, List[String])) : Double = {
    
    //Configure the source (index)
    //val jobConf = SharedESConfig.setupEsOnSparkContext(sc, "test3/nasa3", Some("http://127.0.0.1:9200"))
    
       
    val top_left_lat = regionGeo._2.get(0).split(",")(0).toDouble
    val top_left_lon = regionGeo._2.get(0).split(",")(1).toDouble
    
    val bottom_right_lat = regionGeo._2.get(1).split(",")(0).toDouble
    val bottom_right_lon = regionGeo._2.get(1).split(",")(1).toDouble
    
    println("region: "+ regionGeo._1)
    println("top_left_lat " + top_left_lat)
    println("top_left_lon " + top_left_lon)
    println("bottom_right_lat " + bottom_right_lat)
    println("bottom_right_lon " + bottom_right_lon)
    
    
    val query = "{\"query\": {\"filtered\" :  {\"filter\" : {\"geo_bounding_box\" : {\"location\": { \"top_left\": { \"lat\" :  "+ top_left_lat + ", \"lon\" : " + top_left_lon +"  },\"bottom_right\": { \"lat\":  "+ bottom_right_lat + ", \"lon\": " + bottom_right_lon + "    }}}}}}}"
    println("Using query "+query)
    jobConf.set("es.query", query)   
    
    sc.hadoopRDD(jobConf, classOf[EsInputFormat[Object, MapWritable]], classOf[Object], classOf[MapWritable])
    
    val currentResults = sc.hadoopRDD(jobConf, classOf[EsInputFormat[Object, MapWritable]], classOf[Object], classOf[MapWritable])
    println("currentResults for region: ---> " + regionGeo._1 + "\nRESULTS-->" + currentResults.count())
    
    val meteors = currentResults.map{ case (key, value) => mapWritableToInput(value) }
    
   
//    println("currentResults")
//    println(meteors.flatMap({meteor =>
//      meteor.getOrElse("location", "").split(",")  
//    }).collect().length)  
    
    
    currentResults.count()
    
  }
  
  def mapRegionsToCoordinates() : Map[Double, List[String]] = {
    
    var region1 = List("-180.0,90.0", "-90.0,0.0")
    var region2 = List("-90.0,90.0", "0.0,0.0")
    var region3 = List("0.0,90.0", "90.0,0.0")
    var region4 = List("90.0,90.0", "180.0,0.0")
    var region5 = List("-180.0,0.0", "-90.0,-90.0")
    var region6 = List("-90.0,0.0", "0.0,-90.0")
    var region7 = List("0.0,0.0", "90.0,-90.0")
    var region8 = List("90.0,0.0", "180.0,-90.0")
    
    var regionMap = Map(1.0 -> region1, 2.0 -> region2, 3.0 -> region3, 4.0 -> region4, 5.0 -> region5, 6.0 -> region6,
        7.0 -> region7, 8.0 -> region8)
    
    return regionMap
    
    
  }

}

case class BoundingBox(
  top_left: String,
  bottom_right: String
  ){
}