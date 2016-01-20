package au.edu.uts.rankclass

import org.apache.spark.mllib.linalg.distributed.BlockMatrix
import org.apache.spark.mllib.linalg.Matrix


class RankDistribution(val cat: Integer,val objectType: Integer, val item: Integer, val distribution: Double) extends Serializable {
    var k: Integer = cat //classification area
    var m: Integer = objectType //object type
    var x: Integer = item //specific item in one object
    var p: Double = distribution //ranking distribution
}

class RelationalMatrix(val cat: Integer, val startObj: Integer, val endOjb : Integer, val matrix: BlockMatrix) extends Serializable{
    var k: Integer = cat //classification area
    var i: Integer = startObj //start object type
    var j: Integer = endOjb //end object type
    var mat: BlockMatrix = matrix //normalized matrix
}

class NetworkStructure(val cat: Integer, val matrices: List[Matrix]) {
    var k: Integer = cat //classification area
    var r: List[Matrix] = matrices //specific item in one object
}

