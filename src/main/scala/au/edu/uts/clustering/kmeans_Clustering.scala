package au.edu.uts.clustering

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.clustering.KMeans

object kmeans_Cluster{
  
  @transient
  val conf = new SparkConf()
          .setAppName("kmeans")
          .setMaster("local[16]")
          .set("spark.rdd.compress","true")
//          .set("spark.memory.useLegacyMode","true")
          .set("spark.storage.memoryFraction", "0.9")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.kryoserializer.buffer.max","2000mb")
          .set("spark.kryoserializer.buffer","64mb")
          .set("spark.driver.memory","600gb")
          .set("spark.default.parallelism","32")
          .set("spark.eventLog.enabled","true")

  @transient        
  val sc = new SparkContext(conf)
  
  
  def generateDate(records:Integer) = {
     // Load and parse the data
    
    val prepareDataStart: Long = System.currentTimeMillis / 1000
    val rows = sc.parallelize(List.fill(records)(1))
    val parsedData = rows.map(s => {
          val r = scala.util.Random
          val features = for (i <- 1 to 10000) yield r.nextDouble
          Vectors.dense(features.toArray)
    }).cache()
    
    
    println("---------- data partitions' size : " + rows.partitions.size)
//    println("---------- data partitions' size : " + rows.sparkContext.getConf.get("spark.num.executors"))
    
    val prepareDataEnd: Long = System.currentTimeMillis / 1000
    
    println("---------- Data is ready to run! Spending time: " + (prepareDataEnd-prepareDataStart)/60.0)
    
    // Cluster the data into two classes using KMeans
    val numClusters = 100
    val numIterations = 1
    val clusteringStart: Long = System.currentTimeMillis / 1000
    val clusters = KMeans.train(parsedData, numClusters, numIterations)
    val clusteringEnd: Long = System.currentTimeMillis / 1000
    println("---------- Clustering is finished! Spending time: " + (clusteringEnd-clusteringStart)/60.0)
    
    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)
  }
  
  
  def kmeans() = {
     // Load and parse the data
    val data = sc.textFile("data/mllib/kmeans_data.txt")
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()
    
    // Cluster the data into two classes using KMeans
    val numClusters = 2
    val numIterations = 1
    val clusters = KMeans.train(parsedData, numClusters, numIterations)
    
    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)
  }
    
    // Save and load model
//    clusters.save(sc, "myModelPath")
//    val sameModel = KMeansModel.load(sc, "myModelPath")
    
  def main(args: Array[String]) {
//    kmeans()
    val records = if (args.length > 0) args(0).toInt else 100000
    generateDate(records)
  }
  
}