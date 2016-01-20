package au.edu.uts.metapath

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.mllib.linalg.Vectors
import scala.collection.mutable.ArrayBuffer


object  LoadFile {
  
  var conf = new SparkConf()
          .setAppName("mysql")
          .setMaster("local")
          .set("spark.rdd.compress","true")
          .set("spark.storage.memoryFraction", "1")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.kryoserializer.buffer.max","2000mb")
          .set("spark.kryoserializer.buffer","64M")
          .set("spark.executor.memory","5g")
          
    var sc = new SparkContext(conf)
    var sqlContext = new SQLContext(sc)
    var MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    var MYSQL_USERNAME = "root";
    var MYSQL_PWD = "passwd";
    var MYSQL_CONNECTION_URL = "jdbc:mysql://qcis4:3306/dblp?user=" + MYSQL_USERNAME + "&password=" + MYSQL_PWD;
  
  def loadFile_entries(file:String) = {
      val lines = sc.textFile(file)
      val entries = lines.map { x => {val ele = x.substring(12, x.length()-1).split(",") 
                      new MatrixEntry(ele(0).toLong,ele(1).toLong,ele(2).toDouble)} }.collect()

    
    println( "CoordinateMatrix   ..........")
    val coordMat: CoordinateMatrix = new CoordinateMatrix(sc.parallelize(entries))

    println(coordMat.toBlockMatrix().toLocalMatrix())
    
    }
  
  def loadFile_IndexedRow_From_Word2Vec(file:String) = {
    
    val papersDF = sqlContext.load("jdbc",Map("driver" -> MYSQL_DRIVER, "url" -> MYSQL_CONNECTION_URL,"dbtable" -> "paper_label"))
    
    println( "getting rows ..........")
    val indexedPapers = papersDF.map { x => Map[String, Integer](x.getString(0)-> x.getInt(2)) }.reduce(_++_)
    
    val lines = sc.textFile(file)
    val entries = lines.flatMap { l => 
              val rowValues = new ArrayBuffer[IndexedRow]()
              if(l.startsWith("p_")){
                val line = l.split(" ")
                val index = indexedPapers.get(line(0))
                val elements = line.filter { x => !x.startsWith("p_")}.map { x => x.toDouble}
                rowValues.append(new IndexedRow(index.get.toLong,Vectors.dense(elements)))
              }
            rowValues  
           }

    val indexedRowMat: IndexedRowMatrix = new IndexedRowMatrix(entries)

    indexedRowMat.rows.saveAsTextFile(file+".matrix")
    }
  
  def loadFile_IndexedRow(file:String) = {
    
    val lines = sc.textFile(file)
    
    val entries = lines.flatMap { line => 
              val entryValues = new ArrayBuffer[MatrixEntry]()
              val tempStr = line.substring(11).split(",\\[")
              val rowIndex = tempStr(0).split(",\\(")(0).toInt
              val cols = tempStr(1).substring(0,tempStr(1).length-1).split(",").map { x => x.toInt }
              val values = tempStr(2).substring(0,tempStr(2).length-3).split(",").map { x => x.toDouble }
              for(i <- 0 to cols.length-1){
                entryValues.append(new MatrixEntry(rowIndex, cols(i), values(i)))
              } 
              entryValues  
             }

    val indexedRowMat: CoordinateMatrix = new CoordinateMatrix(entries)

    indexedRowMat.toIndexedRowMatrix().rows.saveAsTextFile(file+".matrix")
    }
 
    
   def main(args: Array[String]) = {
     
//     val file = "/home/xuepeng/uts/data.sample/part-00000"
//     val rowfile = "/home/xuepeng/uts/data/deepwalk-pcp.txt"
     val rowfile = "/home/xuepeng/uts/data.pap/part-00000"
////     loadFile_entries(file)
     loadFile_IndexedRow(rowfile)

   }
}