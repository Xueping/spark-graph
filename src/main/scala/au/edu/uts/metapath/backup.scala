package au.edu.uts.metapath

import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.BlockMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.mllib.linalg.distributed.IndexedRow

object backup {
  def dotProduct(vect:Array[Double]) = {
      vect.map { x => x*x}.reduce(_+_)
    }
  
   def bottom(row:Integer, vect:Array[Double],index:Array[Double]) = {
      vect.zipWithIndex.map { case(e,i) => if(e!=0) 2.0*e/(index(row)+index(i)) else 0}
    }

  
  def main(args: Array[String]) = {
    val conf = new SparkConf()
          .setAppName("mysql")
          .setMaster("local")
          .set("spark.rdd.compress","true")
          .set("spark.storage.memoryFraction", "1")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    
    var MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    var MYSQL_USERNAME = "root";
    var MYSQL_PWD = "passwd";
    var MYSQL_CONNECTION_URL = "jdbc:mysql://qcis4:3306/dblp?user=" + MYSQL_USERNAME + "&password=" + MYSQL_PWD;
    var DBTABLE = "author_sample"
//    var DBTABLE = "author"

    
    var sqlContext = new SQLContext(sc)
    println( "loading data ..........")
    val personDF = sqlContext.load("jdbc",Map("driver" -> MYSQL_DRIVER, "url" -> MYSQL_CONNECTION_URL,"dbtable" -> DBTABLE))
    
    println( "getting rows ..........")
    val rows = personDF.select("name_hash").distinct().map { x => x.getString(0) }.collect()
    println( "getting columns ..........")
    val colums = personDF.select("paper_hash").distinct().map { x => x.getString(0) }.collect()
  
    println( "matrix entry  ..........")
    val rawData = personDF.map { x => new MatrixEntry(rows.indexOf(x.getString(0)),colums.indexOf(x.getString(1)),1.0)}.collect()
//    val rawData = personDF.map { x => new MatrixEntry(rows.indexOf(x.getString(2)),colums.indexOf(x.getString(3)),1.0)}.collect()
    
    println( "RDD entry  ..........")
    val entries: RDD[MatrixEntry] = sc.parallelize(rawData)
    
    println( "CoordinateMatrix   ..........")
    val coordMat: CoordinateMatrix = new CoordinateMatrix(entries)
//    print(coordMat.toBlockMatrix().toLocalMatrix())
    
    println( "BlockMatrix   ..........")
    val matA: BlockMatrix = coordMat.toBlockMatrix()

    //matrix computations: multiple and transpose
    println( "transpose and multiple  ..........")
    val ata = matA.multiply(matA.transpose)
    
//    println(ata.toLocalMatrix())
    
    println( "dia matrix  ..........")
//    //get the diagonal matrix
//    val diagonal = matA.toIndexedRowMatrix().rows.sortBy(x => x.index, true).
//                    map { x => new IndexedRow(x.index,Vectors.dense(Array.fill(matA.numRows().toInt)(dotProduct(x.vector.toArray))))}.collect()
                    
     //get the diagonal matrix
    val diagonal = matA.toIndexedRowMatrix().rows.sortBy(x => x.index, true).map { x => dotProduct(x.vector.toArray)}.collect()
    
    
    println( "computing results entries  ..........")
    val result_IndexRows = ata.toIndexedRowMatrix().rows.map { x => new IndexedRow(x.index,Vectors.dense(bottom(x.index.toInt,x.vector.toArray,diagonal)))}.collect()
   
    println( "results matrix  ..........")
    val result_RowMat: IndexedRowMatrix = new IndexedRowMatrix(sc.parallelize(result_IndexRows))
    
    result_RowMat.toBlockMatrix().toLocalMatrix()
    
//    val diagonal_RowMat: IndexedRowMatrix = new IndexedRowMatrix(sc.parallelize(diagonal))
//                    
//    println(diagonal_RowMat.toBlockMatrix().toLocalMatrix())
//    
//    println(diagonal_RowMat.toBlockMatrix().transpose.toLocalMatrix())
//      
//    val bottom_mat = diagonal_RowMat.toBlockMatrix().add(diagonal_RowMat.toBlockMatrix().transpose)
//    
//    println(bottom_mat.transpose.toLocalMatrix())
//    
//    val bottom_mat_Reciprocal = bottom_mat.toIndexedRowMatrix().rows.map { x => new IndexedRow(x.index,Vectors.dense(bottom(x.vector.toArray)))}.collect()
//    
//    val bottom_mat_Reciprocal_RowMat: IndexedRowMatrix = new IndexedRowMatrix(sc.parallelize(bottom_mat_Reciprocal))
//    
//     println(bottom_mat_Reciprocal_RowMat.toBlockMatrix().toLocalMatrix())
    
//    println( "similarity  ..........")
//    val coordMat_Results = ata.multiply(bottom_mat_Reciprocal_RowMat.toBlockMatrix())
//    
//    println( "similarity  ..........")
//    val coordMat_Results = ata.toCoordinateMatrix().entries.map{ x => new MatrixEntry(x.i,x.j,2.0*x.value/(diagonal(x.i.toInt)+diagonal(x.j.toInt)))}.collect()
  
//    println( "new matrix  ..........")
//    val sim_entries: RDD[MatrixEntry] = sc.parallelize(coordMat_Results)
//    val sim_coordMat: CoordinateMatrix = new CoordinateMatrix(sim_entries)
    
    println( "row and cols   ...")
    println(result_RowMat.numRows()+":"+result_RowMat.numCols())
    println(result_RowMat.toBlockMatrix().toLocalMatrix())
  }
}