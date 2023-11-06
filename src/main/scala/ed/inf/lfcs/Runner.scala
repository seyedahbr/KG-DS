package ed.inf.lfcs.kgds.runner

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import ed.inf.lfcs.kgds.parser._
import ed.inf.lfcs.kgds.planner._

object Runner{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("KnowledgeGraphDistributedSubsetting")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println("STARTING")
    
    val startTime = System.currentTimeMillis()
    
    val program = args(0)
    val pp: ProgramParser = new ProgramParser(spark)
    val parseMap = pp.parseFile(program)
    val planner: ProgramPlanner = new ProgramPlanner(spark)
    val outDF = planner.execute(parseMap)
    
    val outputPath = parseMap("output")
    val ext = outputPath.split("\\.").last    
    val delimiter = ext match {
      case "tsv" => "\t"
      case "nt" => " "
      case _ => ","
    }

    val saveDF = if (ext == "nt") outDF.withColumn("_c3", lit(".")) else outDF
    
    println(s"SAVING OUTPUT IN : $outputPath")

    saveDF.write
      .mode("overwrite")
      .format("csv")      
      .option("header", "false")
      .option("quote", "")
      .option("escape", "") 
      .option("delimiter", delimiter)
      .save(parseMap("output"))   
     
    spark.stop()
  }
}
