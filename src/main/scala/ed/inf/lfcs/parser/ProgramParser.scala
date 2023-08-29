package ed.inf.lfcs.kgds.parser

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.StringBuilder

import scala.io.Source

class ProgramParser(spark: SparkSession){
  private def tokenize(program: String): Map[String, String] = {
    val tokens = program.split("\\s+")
    var keyValuePairs: Map[String, String] = Map.empty

    for (i <- 0 until tokens.length by 2) {
      val key = tokens(i).trim()
      if (i + 1 < tokens.length) {
        val value = tokens(i + 1).trim()
        keyValuePairs += (key -> value)
      }
    }
    keyValuePairs
  }
  
  def parseFile(fileName: String): Map[String, String] = {
    val fileContent = spark.sparkContext.textFile(fileName)
    val rawProgram = fileContent.reduce(_ + " " + _)
    println(rawProgram)
    
    tokenize(rawProgram.toString)    
  }
}