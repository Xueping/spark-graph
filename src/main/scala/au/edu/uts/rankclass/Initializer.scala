package au.edu.uts.rankclass

import scala.tools.nsc.doc.html.page.Index
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.distributed.BlockMatrix
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer


class Initializer extends Serializable{
  
  var init_RD = Array[RankDistribution]()
  var rankDist = Array[RankDistribution]()
  var relationalMat = Array[RelationalMatrix]()
  var normalizedMat = Array[RelationalMatrix]()
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
  
  def initializeZeroRankingDistribution(authorList:Array[String], 
      confList:Array[String], paperList:Array[String],termList:Array[String]) : Array[RankDistribution] = {
    
    var rankDistributions = new ArrayBuffer[RankDistribution]()
    
    for(i <- 0 to authorList.length-1){
      rankDistributions.append(new RankDistribution(0,0,i,0))
      rankDistributions.append(new RankDistribution(1,0,i,0))
      rankDistributions.append(new RankDistribution(2,0,i,0))
      rankDistributions.append(new RankDistribution(3,0,i,0))
    }
    
    for(i <- 0 to confList.length-1){
      rankDistributions.append(new RankDistribution(0,1,i,0))
      rankDistributions.append(new RankDistribution(1,1,i,0))
      rankDistributions.append(new RankDistribution(2,1,i,0))
      rankDistributions.append(new RankDistribution(3,1,i,0))
    }
    
    for(i <- 0 to paperList.length-1){
      rankDistributions.append(new RankDistribution(0,2,i,0))
      rankDistributions.append(new RankDistribution(1,2,i,0))
      rankDistributions.append(new RankDistribution(2,2,i,0))
      rankDistributions.append(new RankDistribution(3,2,i,0))
    }
    
    for(i <- 0 to termList.length-1){
      rankDistributions.append(new RankDistribution(0,3,i,0))
      rankDistributions.append(new RankDistribution(1,3,i,0))
      rankDistributions.append(new RankDistribution(2,3,i,0))
      rankDistributions.append(new RankDistribution(3,3,i,0))
    }
    
    rankDistributions.toArray
  }
  
  def initializeRankingDistribution(sc:SparkContext, labelFile:String, objectList:Array[String], objectType:Int) : Array[RankDistribution] = {
    
    val labelData = sc.textFile(labelFile)
    val labelSum = labelData.map { x => (x.split("\t")(1),1)}.reduceByKey(_ + _ ).collect()
    var labelMap:Map[Int,Int] = Map()
    for(a <- labelSum){
      labelMap += (a._1.toInt -> a._2)
    }
    
    labelData.map { x =>
      new RankDistribution(x.split("\t")(1).toInt,objectType,objectList.indexOf(x.split("\t")(0)),1.0/labelMap(x.split("\t")(1).toInt))
    }.collect()
  }
  
  def D_ij(mat: CoordinateMatrix, sc:SparkContext) : BlockMatrix = {
      val entries = mat.toIndexedRowMatrix().rows.map { x => new MatrixEntry(x.index,x.index, 1.0/Math.sqrt(x.vector.toArray.sum))}.collect()
      new CoordinateMatrix(sc.parallelize(entries)).toBlockMatrix()
    }
  
  def normalizeRelationalMatrix(sc:SparkContext, mat:CoordinateMatrix): BlockMatrix = {
    (D_ij(mat,sc).multiply(mat.toBlockMatrix())).multiply(D_ij(mat.transpose(),sc))
  }

  
  def loadData(){
    
    val authors = "data/rankclass/author.txt"
    val authorData = sc.textFile(authors)
    val authorList = authorData.map { x => x.split("\t")(0)}.collect()
    
    val confs = "data/rankclass/conf.txt"
    val confData = sc.textFile(confs)
    val confList = confData.map { x => x.split("\t")(0)}.collect()
    
    val papers = "data/rankclass/paper.txt"
    val paperData = sc.textFile(papers)
    val paperList = paperData.map { x => x.split("\t")(0)}.collect()
    
    val terms = "data/rankclass/term.txt"
    val termData = sc.textFile(terms)
    val termList = termData.map { x => x.split("\t")(0)}.collect()
    
    val author_label = "data/rankclass/author_label.txt"
    val author_labelList = initializeRankingDistribution(sc,author_label,authorList,0)
    
    val paper_label = "data/rankclass/paper_label.txt"
    val paper_labelList = initializeRankingDistribution(sc,paper_label,paperList,2)

    val conf_label = "data/rankclass/conf_label.txt"
    val conf_labelList = initializeRankingDistribution(sc,conf_label,confList,1)
    
    val labeledList = author_labelList ++ paper_labelList ++ conf_labelList
    val init_zero_RD = initializeZeroRankingDistribution(authorList,confList,paperList,termList)
    init_RD = init_zero_RD.map { x => {
      val matchedRD = labeledList.filter { i => i.k == x.k && i.m == x.m && i.x == x.x}
      if(matchedRD.size == 1)
        matchedRD(0)
      else
        x
    }}
    
    rankDist = init_RD
   
    
    val paper_author = "data/rankclass/paper_author.txt"
    val paper_authorData = sc.textFile(paper_author)
    val paper_authorList = paper_authorData.map { x => new MatrixEntry(paperList.indexOf(x.split("\t")(0)),authorList.indexOf(x.split("\t")(1)),1.0)}.collect()
    val paper_authorEntries: RDD[MatrixEntry] = sc.parallelize(paper_authorList)
    val paper_authorMat: CoordinateMatrix = new CoordinateMatrix(paper_authorEntries)

    val matrices = new ArrayBuffer[RelationalMatrix]()
    val normalizedMatrices = new ArrayBuffer[RelationalMatrix]()
    
    matrices.append(new RelationalMatrix(0,2,0,paper_authorMat.toBlockMatrix()))
    matrices.append(new RelationalMatrix(1,2,0,paper_authorMat.toBlockMatrix()))
    matrices.append(new RelationalMatrix(2,2,0,paper_authorMat.toBlockMatrix()))
    matrices.append(new RelationalMatrix(3,2,0,paper_authorMat.toBlockMatrix()))
    
  
    matrices.append(new RelationalMatrix(0,0,2,paper_authorMat.transpose().toBlockMatrix()))
    matrices.append(new RelationalMatrix(1,0,2,paper_authorMat.transpose().toBlockMatrix()))
    matrices.append(new RelationalMatrix(2,0,2,paper_authorMat.transpose().toBlockMatrix()))
    matrices.append(new RelationalMatrix(3,0,2,paper_authorMat.transpose().toBlockMatrix()))
    

    val paper_authorNormalMat = normalizeRelationalMatrix(sc,paper_authorMat)
    normalizedMatrices.append(new RelationalMatrix(0,2,0,paper_authorNormalMat))
    normalizedMatrices.append(new RelationalMatrix(1,2,0,paper_authorNormalMat))
    normalizedMatrices.append(new RelationalMatrix(2,2,0,paper_authorNormalMat))
    normalizedMatrices.append(new RelationalMatrix(3,2,0,paper_authorNormalMat))
    
  
    normalizedMatrices.append(new RelationalMatrix(0,0,2,paper_authorNormalMat.transpose))
    normalizedMatrices.append(new RelationalMatrix(1,0,2,paper_authorNormalMat.transpose))
    normalizedMatrices.append(new RelationalMatrix(2,0,2,paper_authorNormalMat.transpose))
    normalizedMatrices.append(new RelationalMatrix(3,0,2,paper_authorNormalMat.transpose))
    
    val paper_conf = "data/rankclass/paper_conf.txt"
    val paper_confData = sc.textFile(paper_conf)
    val paper_confList = paper_confData.map { x => new MatrixEntry(paperList.indexOf(x.split("\t")(0)),confList.indexOf(x.split("\t")(1)),1.0)}.collect()
    val paper_confEntries: RDD[MatrixEntry] = sc.parallelize(paper_confList)
    val paper_confMat: CoordinateMatrix = new CoordinateMatrix(paper_confEntries)
    
    val S_paper_conf = paper_confMat.toBlockMatrix()
    matrices.append(new RelationalMatrix(0,2,1,S_paper_conf))
    matrices.append(new RelationalMatrix(1,2,1,S_paper_conf))
    matrices.append(new RelationalMatrix(2,2,1,S_paper_conf))
    matrices.append(new RelationalMatrix(3,2,1,S_paper_conf))
   
    val S_conf_paper = S_paper_conf.transpose
    matrices.append(new RelationalMatrix(0,1,2,S_conf_paper))
    matrices.append(new RelationalMatrix(1,1,2,S_conf_paper))
    matrices.append(new RelationalMatrix(2,1,2,S_conf_paper))
    matrices.append(new RelationalMatrix(3,1,2,S_conf_paper))
    
    val paper_confMatNormalMat = normalizeRelationalMatrix(sc,paper_confMat)
    normalizedMatrices.append(new RelationalMatrix(0,2,0,paper_confMatNormalMat))
    normalizedMatrices.append(new RelationalMatrix(1,2,0,paper_confMatNormalMat))
    normalizedMatrices.append(new RelationalMatrix(2,2,0,paper_confMatNormalMat))
    normalizedMatrices.append(new RelationalMatrix(3,2,0,paper_confMatNormalMat))
    
  
    normalizedMatrices.append(new RelationalMatrix(0,0,2,paper_confMatNormalMat.transpose))
    normalizedMatrices.append(new RelationalMatrix(1,0,2,paper_confMatNormalMat.transpose))
    normalizedMatrices.append(new RelationalMatrix(2,0,2,paper_confMatNormalMat.transpose))
    normalizedMatrices.append(new RelationalMatrix(3,0,2,paper_confMatNormalMat.transpose))
    
   
    val paper_term = "data/rankclass/paper_term.txt"
    val paper_termData = sc.textFile(paper_term)
    val paper_termList = paper_termData.map { x => new MatrixEntry(paperList.indexOf(x.split("\t")(0)),termList.indexOf(x.split("\t")(1)),1.0)}.collect()
    val paper_termEntries: RDD[MatrixEntry] = sc.parallelize(paper_termList)
    val paper_termMat: CoordinateMatrix = new CoordinateMatrix(paper_termEntries)
    
    val S_paper_term = paper_termMat.toBlockMatrix()
    matrices.append(new RelationalMatrix(0,2,3,S_paper_term))
    matrices.append(new RelationalMatrix(1,2,3,S_paper_term))
    matrices.append(new RelationalMatrix(2,2,3,S_paper_term))
    matrices.append(new RelationalMatrix(3,2,3,S_paper_term))
    
    val S_term_paper = S_paper_term.transpose
    matrices.append(new RelationalMatrix(0,3,2,S_term_paper))
    matrices.append(new RelationalMatrix(1,3,2,S_term_paper))
    matrices.append(new RelationalMatrix(2,3,2,S_term_paper))
    matrices.append(new RelationalMatrix(3,3,2,S_term_paper))
    
    val paper_termMatNormalMat = normalizeRelationalMatrix(sc,paper_termMat)
    normalizedMatrices.append(new RelationalMatrix(0,2,0,paper_termMatNormalMat))
    normalizedMatrices.append(new RelationalMatrix(1,2,0,paper_termMatNormalMat))
    normalizedMatrices.append(new RelationalMatrix(2,2,0,paper_termMatNormalMat))
    normalizedMatrices.append(new RelationalMatrix(3,2,0,paper_termMatNormalMat))
    
  
    normalizedMatrices.append(new RelationalMatrix(0,0,2,paper_termMatNormalMat.transpose))
    normalizedMatrices.append(new RelationalMatrix(1,0,2,paper_termMatNormalMat.transpose))
    normalizedMatrices.append(new RelationalMatrix(2,0,2,paper_termMatNormalMat.transpose))
    normalizedMatrices.append(new RelationalMatrix(3,0,2,paper_termMatNormalMat.transpose))
    
    relationalMat = matrices.toArray;
    normalizedMat = normalizedMatrices.toArray
  }
  
  def getRankDistribution(relationalMats: Array[RelationalMatrix], 
      rankDist : Array[RankDistribution], init_RD : Array[RankDistribution]) : Array[RankDistribution] = {
    
//    val init_RD_Para = sc.parallelize(init_RD)
    
    init_RD.map { x => { 
      val lambda = 0.2
      val alpha = 0.1
      
      var firstTerm = 0.0
      for(m <- 0 to 3){
        //get the normalized matrices through filtering the classification, start and end object type
        val current_cat_obj_S = relationalMats.filter { s => s.k == x.k && s.i == x.m && s.j == m}
        if (current_cat_obj_S.length > 0){
          
          firstTerm += current_cat_obj_S(0).mat.toIndexedRowMatrix().rows.filter { item => item.index == x.item }.map { item => 
            {
              
              var itemSum = 0.0
              val neighbors = item.vector.toArray
              for(nj <- 0 to neighbors.length-1){
                //get the ranking distributions through filtering the classifications,object type, and id
                val neighbor_RD = rankDist.filter { n => n.k == x.k && n.m == m && n.item == nj }
                if (neighbor_RD.size > 0){
                  itemSum += neighbor_RD(0).p*neighbors(nj)
                  }
                }
              itemSum
            }
          }.reduce(_+_)
        }
      }
      val secondTerm = alpha * x.p
      val thirdTerm = 4 * lambda + alpha
      val updateRD = (firstTerm + secondTerm) / thirdTerm
      new RankDistribution(x.k,x.m,x.x,updateRD)
      } 
    }
  }
  
  def getNetworkStructure(relationalMats: Array[RelationalMatrix], rankDist : Array[RankDistribution], t : Int) : Array[RelationalMatrix] = {
    relationalMats.map { x => new RelationalMatrix(x.k,x.i,x.j,x.mat.multiply(rankDist, x, t))}
  }
  

  def execReinforce() = {
    
    for(t <- 1 to 5){
      rankDist = getRankDistribution(normalizedMat,rankDist,init_RD)
      relationalMat = getNetworkStructure(relationalMat,rankDist,t)
      normalizedMat = relationalMat.map { x => new RelationalMatrix(x.k,x.i,x.j,normalizeRelationalMatrix(sc,x.mat.toCoordinateMatrix())) }
    }
  }
}