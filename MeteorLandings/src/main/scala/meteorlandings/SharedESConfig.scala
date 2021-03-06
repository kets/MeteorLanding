package meteorlandings

// Scala imports
import scala.collection.JavaConversions._
// Spark imports
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.elasticsearch.spark._
// ES imports
import org.elasticsearch.hadoop.mr.EsOutputFormat
import org.elasticsearch.hadoop.mr.EsInputFormat
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
// Hadoop imports
import org.apache.hadoop.mapred.{FileOutputCommitter, FileOutputFormat, JobConf, OutputFormat}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{MapWritable, Text, NullWritable}
import org.elasticsearch.spark.rdd


object SharedESConfig {
  def setupEsOnSparkContext(sc: SparkContext, esResource: String, esNodes: Option[String] = None,
    esSparkPartition: Boolean = false) = {
    println("creating configuration for "+esResource+" on "+esNodes)
    val jobConf = new JobConf(sc.hadoopConfiguration)
    jobConf.set("mapred.output.format.class", "org.elasticsearch.hadoop.mr.EsOutputFormat")
    jobConf.setOutputCommitter(classOf[FileOutputCommitter])
    jobConf.set(ConfigurationOptions.ES_RESOURCE, esResource)
    
//    sc.addJar("target/scala-2.10/meteors-landings_2.10-1.0.jar")
//    sc.addJar("lib/elasticsearch-2.2.0.jar")
//    sc.addJar("lib/elasticsearch-hadoop-2.2.0.jar")
//    sc.addJar("lib/elasticsearch-spark_2.11-2.2.0-rc1.jar")
//    sc.addJar("lib/spark-assembly-1.6.0-hadoop2.6.0.jar")
    
    esNodes match {
      case Some(node) => jobConf.set(ConfigurationOptions.ES_NODES, node)
      case _ => // Skip it
    }
    // This tells the ES MR output format to use the spark partition index as the node index
    // This will degrade performance unless you have the same partition layout as ES in which case
    // it should improve performance.
    // Note: this is currently implemented as kind of a hack.
    if (esSparkPartition) {
      jobConf.set("es.sparkpartition", "true")
    }
    FileOutputFormat.setOutputPath(jobConf, new Path("-"))
    jobConf
  }
}