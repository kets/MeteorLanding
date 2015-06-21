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
import org.elasticsearch.common.geo.GeoPoint;


object NaiveBayesSample {
  
  def main(args: Array[String]){
    val conf = new SparkConf
    conf.setMaster("spark://localhost:7077")
    conf.setAppName("MeteorLandings")
    val sc = new SparkContext(conf)    
      
    
    //val query = "{\"query\": {\"filtered\" : {\"filter\" : {\"range\" : {\"year\": { \"gte\": \"1600\", \"lte\" : \"2050\" }}}}}}"
    val query = "{\"query\": {\"filtered\" :  {\"filter\" : {\"geo_bounding_box\" : {\"location\": { \"top_left\": { \"lat\" :  "+ 0.0 + ", \"lon\" : " + 90.0 +"  },\"bottom_right\": { \"lat\":  "+ -180.0 + ", \"lon\": " + -90.0 + "    }}}}}}}"
    
    //val query = "{\"query\": {\"filtered\" : {\"query\" : {\"match_all\" : {}}}}}"
    val jobConf = SharedESConfig.setupEsOnSparkContext(sc, "test3/nasa3", Some("http://127.0.0.1:9200"))
//    
//    val esRDD = sc.esRDD("test3/nasa3", query)
//      
   

    // get the impacts per region    
    val regionImpacts =  mapRegionsToCoordinates().map({ line =>
      getImpactsByRegion(sc, jobConf, line)
      })
      
    val flatRegion = regionImpacts.flatten
    
    val rdd = flatRegion.map({
      x => LabeledPoint(x._1, Vectors.dense(x._2)
    )})
    
    //convert map to RDD
    val rddRegionImpacts = sc.parallelize(rdd.toSeq)
    
    println("---rddRegionImpacts--- " + rddRegionImpacts.count)
    
    
//    //create the labeled points and vectors
//    val parsedData = rddRegionImpacts.map({x =>
//      x.map({case(key, value) =>
//        LabeledPoint(key, Vectors.dense(value))         
//      })      
//    })
    
   
   

    
   // println("----parsedData-----"+ parsedData.count())
   
    
    trainNaiveBayes(rddRegionImpacts)
    
  }
  
  private def trainNaiveBayes(rddRegionImpacts: RDD[LabeledPoint]){
    
     // Split data into training (60%) and test (40%).
    val splits = rddRegionImpacts.randomSplit(Array(0.6, 0.4), seed = 11L)
    println("----rddRegionImpacts-----"+ rddRegionImpacts.count())
    val training = splits(0)
    val test = splits(1)

    println("----training-----"+ training.count())
    
    println("----test-----"+ test.count())
    
    val model = NaiveBayes.train(training, lambda = 1.0)
    
    println("model: "+ model.labels.length)
    
    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
    
    println("----predictionAndLabel-----"+ predictionAndLabel.count())
    
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    
    println("accuracy: " + accuracy)
    
  }
  
  private def toVector(data:Array[String], fields:Array[String]):Vector = {

    val lat = data(0).toDouble
    val lon = data(1).toDouble
  
    Vectors.dense(Array(lat,lon))

  }
    
  def mapWritableToInput(in: MapWritable): Map[String, String] = {
 
    in.map{case (k, v) => (k.toString, v.toString)}.toMap
  }
  
  
  def getImpactsByRegion(sc : SparkContext, jobConf : JobConf, regionGeo : Map[Double, List[GeoPoint]]): Map[Double,Double] = {
     val regionImpacts = mapRegionsToCoordinates().map({ line =>
      getImpactsByRegion(sc, jobConf, line)
      }).flatten.toSeq
   
      val rdd = sc.parallelize(regionImpacts)
    
   
    Map(0.0 -> 0.0)
    
  }
  
  def getImpactsByRegion(sc : SparkContext, jobConf : JobConf, regionGeo : (Double, List[GeoPoint])) : Map[Double,Double] = {
    
  //Configure the source (index)
  //val jobConf = SharedESConfig.setupEsOnSparkContext(sc, "test3/nasa3", Some("http://127.0.0.1:9200"))
  
     
//    val top_left_lat = regionGeo._2.get(0).getLat
//    val top_left_lon = regionGeo._2.get(0).getLon    
//    val bottom_right_lat = regionGeo._2.get(1).getLat
//    val bottom_right_lon = regionGeo._2.get(1).getLon
    
    val topLeft = regionGeo._2.get(0)
    val bottomRight = regionGeo._2.get(1)
  
//    println("region: "+ regionGeo._1)
//    println("top_left " + top_left_lat)
//    println("top_left_lon " + top_left_lon)
//    println("bottom_right " + bottom_right_lat)
//    println("bottom_right_lon " + bottom_right_lon)
    
    
    val query = "{\"query\": {\"filtered\" :  {\"filter\" : {\"geo_bounding_box\" : {\"location\": { \"top_left\":  "+ topLeft  + ", \"bottom_right\":   "+ bottomRight + "    }}}}}}}"
    //val query = "{\"query\": {\"filtered\" :  {\"filter\" : {\"geo_bounding_box\" : {\"location\": { \"top_left\": { \"lat\" :  "+ top_left_lat + ", \"lon\" : " + top_left_lon +"  },\"bottom_right\": { \"lat\":  "+ bottom_right_lat + ", \"lon\": " + bottom_right_lon+ "    }}}}}}}"
    println("Using query "+query)
    jobConf.set("es.query", query)   
    
      // Read from ES using inputformat from org.elasticsearch.hadoop;
     // note, that key [Object] specifies the document id (_id) and
     // value [MapWritable] the document as a field -> value map (location -> "34.45,23.45"
    
    sc.hadoopRDD(jobConf, classOf[EsInputFormat[Object, MapWritable]], classOf[Object], classOf[MapWritable])
    
    val currentResults = sc.hadoopRDD(jobConf, classOf[EsInputFormat[Object, MapWritable]], classOf[Object], classOf[MapWritable])
    println("currentResults for region: ---> " + regionGeo._1 + "\nRESULTS-->" + currentResults.count())
    
    val meteors = currentResults.map{ case (key, value) => mapWritableToInput(value) }
        
    
    val regionByImpacts = Map(regionGeo._1 -> currentResults.count().toDouble)
    return regionByImpacts
    
  }
  
  def mapRegionsToCoordinates() : Map[Double, List[GeoPoint]] = {        
    
    var region1 = List(new GeoPoint(-180.0,90.0), new GeoPoint(-90.0,0.0))
    var region2 = List(new GeoPoint(-90.0,90.0), new GeoPoint(0.0,0.0))
    var region3 = List(new GeoPoint(0.0,90.0), new GeoPoint(90.0,0.0))
    var region4 = List(new GeoPoint(90.0,90.0), new GeoPoint(180.0,0.0))
    var region5 = List(new GeoPoint(-180.0,0.0), new GeoPoint(-90.0,-90.0))
    var region6 = List(new GeoPoint(-90.0,0.0), new GeoPoint(0.0,-90.0))
    var region7 = List(new GeoPoint(0.0,0.0), new GeoPoint(90.0,-90.0))
    var region8 = List(new GeoPoint(90.0,0.0), new GeoPoint(180.0,-90.0))
    
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