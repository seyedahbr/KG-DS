package ed.inf.lfcs.kgds.runner

import org.apache.spark.sql.SparkSession

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
    val rdd = planner.execute(parseMap)    
    rdd.saveAsTextFile(parseMap("output"))
     
    spark.stop()
  }
}
