package au.edu.uts.metapath

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.BlockMatrix
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.feature.ElementwiseProduct
import breeze.linalg.DenseMatrix
//import scala.collection.mutable.{Map, HashMap}

object  Metapath {
  
    var MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    var MYSQL_USERNAME = "root";
    var MYSQL_PWD = "passwd";
    var MYSQL_CONNECTION_URL = "jdbc:mysql://qcis4:3306/dblp?user=" + MYSQL_USERNAME + "&password=" + MYSQL_PWD;
    var DBTABLE_pap = "pap"
    var DBTABLE_pcp = "pcp"
    var DBTABLE = "author_sample"
//    var DBTABLE = "db_author"
//    var DBTABLE = "dm_author"
//    var DBTABLE = "cv_author"
//    var DBTABLE = "ai_author"
  
  def dotProduct(vect:Array[Double]) = {
      vect.map { x => x*x}.reduce(_+_)
    }

  
   def bottom(row:Integer, vect:Array[Double],index:Array[Double]) = {
      vect.zipWithIndex.map { case(e,i) => if(e!=0) 2.0*e/(index(row)+index(i)) else 0}
    }

   def sample(path:String,sc:SparkContext, sqlContext:SQLContext) = {
    
    println( "loading data ..........")
    val personDF = sqlContext.load("jdbc",Map("driver" -> MYSQL_DRIVER, "url" -> MYSQL_CONNECTION_URL,"dbtable" -> path))
    val papersDF = sqlContext.load("jdbc",Map("driver" -> MYSQL_DRIVER, "url" -> MYSQL_CONNECTION_URL,"dbtable" -> "paper_label"))
    
    println( "getting rows ..........")
    val colums = personDF.select("paper_hash").distinct().map { x => x.getString(0) }.collect()
    println( "getting columns ..........")
    val rows = personDF.select("name_hash").distinct().map { x => x.getString(0) }.collect()
  
    println( "matrix entry  ..........")
    val rawData = personDF.map { x => new MatrixEntry(rows.indexOf(x.getString(0)),colums.indexOf(x.getString(1)),1.0)}.collect()
    
  
    
    println( "RDD entry  ..........")
    val entries: RDD[MatrixEntry] = sc.parallelize(rawData)
    
    println( "CoordinateMatrix   ..........")
    val coordMat: CoordinateMatrix = new CoordinateMatrix(entries)
    
    println( "BlockMatrix   ..........")
    val matA: BlockMatrix = coordMat.toBlockMatrix().cache()

    //matrix computations: multiple and transpose
    println( "transpose and multiple  ..........")
    val ata = matA.multiply(matA.transpose)
      println( "number of blocks :"+ata.blocks.count())

    println( "row and cols   ...")
//    val diagonal = matA.toIndexedRowMatrix().rows.sortBy(x => x.index, true).map { x => dotProduct(x.vector.toArray)}.collect()
     val diagonal = matA.toIndexedRowMatrix().rows.map {x => Map(x.index -> dotProduct(x.vector.toArray))}.reduce(_++_)
//    val indexedMat: IndexedRowMatrix = new IndexedRowMatrix(sc.parallelize(diagonal))
                    
//    println("adding two matrices together ....." )
//    val diagonalMat = indexedMat.toBlockMatrix().add(indexedMat.toBlockMatrix().transpose)
    
    println( "calculating the results......")    
    val result_RowMat = ata.divide(diagonal)

//    val result_RowMat = sparkOperation(matA,sc, ata)
    println(result_RowMat.numRows()+":"+result_RowMat.numCols())
    println(result_RowMat.toLocalMatrix())
//    result_RowMat.toIndexedRowMatrix().rows.saveAsTextFile("/home/xuepeng/uts/data."+path)
  }
  
  def pap(path:String, sc:SparkContext, sqlContext:SQLContext) = {
    
    println( "loading data ..........")
    val papDF = sqlContext.load("jdbc",Map("driver" -> MYSQL_DRIVER, "url" -> MYSQL_CONNECTION_URL,"dbtable" -> path))
    val papersDF = sqlContext.load("jdbc",Map("driver" -> MYSQL_DRIVER, "url" -> MYSQL_CONNECTION_URL,"dbtable" -> "paper_label"))
    
    println( "getting rows ..........")
    val rows = papersDF.map { x => Map[String, Integer](x.getString(0)-> x.getInt(2)) }.reduce(_++_)
    println( "getting columns ..........")
    val colums = papDF.select("name_hash").distinct().map { x => x.getString(0) }.collect()
  
    println( "matrix entry  ..........")
    val rawData = papDF.map { x => new MatrixEntry(rows.get(x.getString(1)).get.toLong,colums.indexOf(x.getString(0)),1.0)}.collect()
    
    println( "RDD entry  ..........")
    val entries: RDD[MatrixEntry] = sc.parallelize(rawData)
    
    println( "CoordinateMatrix   ..........")
    val coordMat: CoordinateMatrix = new CoordinateMatrix(entries)
    
    println( "BlockMatrix   ..........")
    val matA: BlockMatrix = coordMat.toBlockMatrix().cache()

    //matrix computations: multiple and transpose
    println( "transpose and multiple  ..........")
    val ata = matA.multiply(matA.transpose)
    
    println( "dia matrix  ..........")
    val diagonal = coordMat.toIndexedRowMatrix().rows.map {x => Map(x.index -> dotProduct(x.vector.toArray))}.reduce(_++_)
    println( "calculating the results......")    
    val result_RowMat = ata.divide(diagonal)

//    val result_RowMat = sparkOperation(matA,sc, ata)
    println(result_RowMat.numRows()+":"+result_RowMat.numCols())
    result_RowMat.toIndexedRowMatrix().rows.saveAsTextFile("/home/xuepeng/uts/data."+path)
    
  }
  
  def pcp(path:String, sc:SparkContext, sqlContext:SQLContext) = {
    
    println( "loading data ..........")
    val personDF = sqlContext.load("jdbc",Map("driver" -> MYSQL_DRIVER, "url" -> MYSQL_CONNECTION_URL,"dbtable" -> path))
    val papersDF = sqlContext.load("jdbc",Map("driver" -> MYSQL_DRIVER, "url" -> MYSQL_CONNECTION_URL,"dbtable" -> "paper_label"))
    
    println( "getting rows ..........")
    val rows = papersDF.map { x => Map[String, Integer](x.getString(0)-> x.getInt(2)) }.reduce(_++_)
    
    println( "getting columns ..........")
    val colums = personDF.select("conference").distinct().map { x => x.getString(0) }.collect()
  
    println( "matrix entry  ..........")
    val rawData = personDF.map { x => new MatrixEntry(rows.get(x.getString(0)).get.toLong,colums.indexOf(x.getString(1)),1.0)}.collect()
    
    println( "RDD entry  ..........")
    val entries: RDD[MatrixEntry] = sc.parallelize(rawData)
    
    println( "CoordinateMatrix   ..........")
    val coordMat: CoordinateMatrix = new CoordinateMatrix(entries)
    
    println( "BlockMatrix   ..........")
    val matA: BlockMatrix = coordMat.toBlockMatrix().cache()

    //matrix computations: multiple and transpose
    println( "transpose and multiple  ..........")
    val ata = matA.multiply(matA.transpose)

    println( "dia matrix  ..........")
    val diagonal = matA.toIndexedRowMatrix().rows.map {x => Map(x.index -> dotProduct(x.vector.toArray))}.reduce(_++_)

    println( "calculating the results......")    
    val result_RowMat = ata.divide(diagonal)

//    val result_RowMat = sparkOperation(matA,sc, ata)
    println(result_RowMat.numRows()+":"+result_RowMat.numCols())
    result_RowMat.toIndexedRowMatrix().rows.saveAsTextFile("/home/xuepeng/uts/data."+path)
    
  }

  
   def main(args: Array[String]) = {
     
     val conf = new SparkConf()
          .setAppName("mysql")
          .setMaster("local")
          .set("spark.rdd.compress","true")
          .set("spark.storage.memoryFraction", "1")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.kryoserializer.buffer.max","2000mb")
          .set("spark.kryoserializer.buffer","64M")
          .set("spark.executor.memory","5g")
          
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    pap("pap",sc, sqlContext)
//    pcp("pcp",sc, sqlContext)
//    sample("author_sample",sc, sqlContext)
   
     
   }
  
}