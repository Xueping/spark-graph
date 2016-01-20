package au.edu.uts.rankclass


object RankClass extends Serializable{
  
  def main(args: Array[String]) = {
    val init = new Initializer()
    init.loadData()
    init.execReinforce()
    
     
    for (i <-  init.init_RD){
      println( i.k, i.m, i.x,i.p)
    }
    
    for (i <-  init.rankDist){
      println( i.k, i.m, i.x,i.p)
    }
    
    
    for (x <-  init.relationalMat){
      println( x.k, x.i, x.j,x.mat.numCols(),x.mat.numRows())
    }
    
  }
  
}