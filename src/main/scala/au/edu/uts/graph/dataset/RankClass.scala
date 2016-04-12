package au.edu.uts.graph.dataset

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Graph

object RankClass {
  
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
  var accum = sc.accumulator(0, "index of vertex")
  
  def loadData(){
    
    
    val labeledAuthors = "data/rankclass/author_label.txt"
    val labeledAuthorList = sc.textFile(labeledAuthors).map { x => Map("A"+x.split("\t")(0).toString() -> "L".+(x.split("\t")(1).toString()) )}.reduce(_++_)
    
    val authors = "data/rankclass/author.txt"
    val authorVertices = sc.textFile(authors).map { x =>{
          accum += 1
          Map("A"+x.split("\t")(0).toString() -> (accum.value.toLong, ("Author",x.split("\t")(0).toString(),{
          if(labeledAuthorList.contains("A"+x.split("\t")(0).toString()))
             labeledAuthorList("A"+x.split("\t")(0).toString())
          else
            "L00"
        } , x.split("\t")(1))))
      }
      
     }.reduce(_++_)
    
    
    val labeledPapers = "data/rankclass/paper_label.txt"
    val labeledPaperList = sc.textFile(labeledPapers).map { x => Map("P"+x.split("\t")(0).toString() -> "L".+(x.split("\t")(1).toString()) )}.reduce(_++_)
    
    val papers = "data/rankclass/paper.txt"
    val paperVertices = sc.textFile(papers).map { x =>  {
      
       accum += 1
          Map("P"+x.split("\t")(0).toString() -> (accum.value.toLong, ("Paper",x.split("\t")(0).toString(),{
             
          if(labeledPaperList.contains("P"+x.split("\t")(0).toString()))
             labeledPaperList("P"+x.split("\t")(0).toString())
          else
            "L00"
        } , x.split("\t")(1))))
      }
    
    }.reduce(_++_)
    
    val labeledConfs = "data/rankclass/conf_label.txt"
    val confVertices = sc.textFile(labeledConfs).map { x => {
      accum += 1
       Map("C"+x.split("\t")(0).toString() -> 
      (accum.value.toLong,("Conf",x.split("\t")(0).toString(), "L".+(x.split("\t")(1).toString()), x.split("\t")(2).toString() )))}
    }.reduce(_++_)
        
    val terms = "data/rankclass/term.txt"
    val termVertices = sc.textFile(terms).map { x => {
      accum += 1
       Map("T"+x.split("\t")(0).toString() -> 
      (accum.value.toLong,("Term",x.split("\t")(0).toString(), "L00", x.split("\t")(1))))
        }
      }.reduce(_++_)

    val mapVertices = authorVertices ++ paperVertices ++ confVertices ++ termVertices
    
    val totalVertices = mapVertices.values.toArray
    val vertices: RDD[(Long, (String, String,String, String))] = sc.parallelize(totalVertices)
    
    val paper_author = "data/rankclass/paper_author.txt"
    val paper_authorData = sc.textFile(paper_author)
    val paper_authors = paper_authorData.map { x => Edge(mapVertices("P"+x.split("\t")(0).toString())._1,mapVertices("A"+x.split("\t")(1).toString())._1,"write")}.collect()
    
    val paper_conf = "data/rankclass/paper_conf.txt"
    val paper_confData = sc.textFile(paper_conf)
    val paper_Confs = paper_confData.map { x => Edge(mapVertices("P"+x.split("\t")(0).toString())._1,mapVertices("C"+x.split("\t")(1).toString())._1,"publish")}.collect()
    
    val paper_term = "data/rankclass/paper_term.txt"
    val paper_termData = sc.textFile(paper_term)
    val paper_terms = paper_termData.map { x => Edge(mapVertices("P"+x.split("\t")(0).toString())._1,mapVertices("T"+x.split("\t")(1).toString())._1,"contain")}.collect()
    
    val totalEdges = paper_authors ++ paper_Confs ++ paper_terms
    val edges:RDD[Edge[String]]  = sc.parallelize(totalEdges)
    
    val graph = Graph(vertices, edges)
    
    println(graph.numVertices)
    println(graph.numEdges)

    
  }
  
  def main(args: Array[String]) = {
    loadData()
  }
  
}