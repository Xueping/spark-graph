package au.edu.uts.metapath

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount { 
    
  def main(args: Array[String]) = {
//    val conf = new SparkConf()
//          .setAppName("WordCount")
//          .setMaster("local")
//    val sc = new SparkContext(conf)
//    
//    val test = sc.textFile("food.txt")
//    test.flatMap { line => line.split(" ") }
//        .map { word => (word,1) }
//        .reduceByKey(_ + _ )
//        .saveAsTextFile("food.count.txt")
  
    val list = Array.fill(3)(10.0)
    for(l <- list){
       print(l)
    }
     
  }
}