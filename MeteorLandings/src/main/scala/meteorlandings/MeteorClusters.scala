package meteorlandings

import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.feature._
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
  
    val conf = new SparkConf().setAppName("MeteorClusters").setMaster("spark://localhost:7077")
    conf.set("es.index.auto.create", "true");
    val sc = new SparkContext(conf)    
    
    sc.addJar("target/scala-2.10/meteors-landings_2.10-1.0.jar")
//    sc.addJar("lib/elasticsearch-2.2.0.jar")
//    sc.addJar("lib/elasticsearch-hadoop-2.2.0.jar")
//    sc.addJar("lib/elasticsearch-spark_2.11-2.2.0-rc1.jar")
//    sc.addJar("lib/spark-assembly-1.6.0-hadoop2.6.0.jar")
    //Configure the source (index)
    val jobConf = SharedESConfig.setupEsOnSparkContext(sc, "test3/nasa3", Some("localhost:9200"))
    //val indexRdd = sc.esRDD()
    
    val query = "{\"query\": {\"filtered\" : {\"filter\" : {\"range\" : {\"year\": { \"gte\": \"1700\", \"lte\" : \"2050\" }}}}}}"
    //val query = "{\"query\": {\"filtered\" : {\"query\" : {\"match_all\" : {}}}}}"
    println("Using query "+query)
    //jobConf.set("es.query", query)
    
    val esRDD = sc.esRDD("test3/nasa3", query)
    //println("query count: "+ esRDD.foreach(lines => println()))
    
   // Read from ES using inputformat from org.elasticsearch.hadoop;
   // note, that key [Object] specifies the document id (_id) and
   // value [MapWritable] the document as a field -> value map (location -> "34.45,23.45"
      val currentResults = sc.hadoopRDD(jobConf, classOf[EsInputFormat[Object, MapWritable]], classOf[Object], classOf[MapWritable])
      println("currentResults count: "+ currentResults.count())
      val meteors = currentResults.map{ case (key, value) => mapWritableToInput(value) }
      //val meteors = sqlCtx.esRDD("test/nasa", query)
       
    
    val fields = new Array[String](500)
    
    // Extract the geelocation from each document and create a vector
    val vectors = meteors.map(meteor => toVector(meteor.getOrElse("location", "").split(","), fields))
    
    // Cluster the data into two classes using KMeans
    val numClusters = 3
    val numIterations = 10
    
    //TODO: check if we can train using another field
    //train the model
    val model = KMeans.train(vectors, numClusters, numIterations)
     
    val clusterCenters = model.clusterCenters.map(_.toArray)
    
    val cost = model.computeCost(vectors)
    println("cost: "+ cost)
    
    val meteorsByGroup = meteors.map{meteor => 
      meteor.getOrElse("location", "").split(",").map(_.toDouble)
      }.groupBy { rdd => 
        model.predict(Vectors.dense(rdd)) 
        }.collect()
    
    //TODO
    meteorsByGroup.map(x => x._2.flatten).foreach(println)
    
    println(vectors.countByValue())
  
    //val vectors = Vectors.dense(location)
     
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