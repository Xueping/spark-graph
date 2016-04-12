package au.edu.uts.graph.dataset

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix

object SparseMatrix {
  
  @transient
  val conf = new SparkConf()
          .setAppName("mysql")
          .setMaster("local")
          .set("spark.rdd.compress","true")
          .set("spark.storage.memoryFraction", "1")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.kryoserializer.buffer.max","2000mb")
          .set("spark.kryoserializer.buffer","64M")
          .set("spark.executor.memory","5g")
  @transient        
  val sc = new SparkContext(conf)
  
  
  
  def main(args: Array[String]) = {
    val rawMatrix = "/home/xuepeng/uts/dataset/matrix.csv"
    val rawData = sc.textFile(rawMatrix).map { x => new MatrixEntry(x.split(",")(0).toLong-1,x.split(",")(1).toLong-1,x.split(",")(2).toDouble)}.collect()
    val entries: RDD[MatrixEntry] = sc.parallelize(rawData)
    val coordMat: CoordinateMatrix = new CoordinateMatrix(entries)
    
    println(coordMat.numCols())
    println(coordMat.numRows())
    
    val mulMat = coordMat.toBlockMatrix().multiply(coordMat.toBlockMatrix().transpose)
    
    println(mulMat.numCols())
    println(mulMat.numRows())
    
  }
   
}