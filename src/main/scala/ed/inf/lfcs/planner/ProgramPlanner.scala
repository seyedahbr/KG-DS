package ed.inf.lfcs.kgds.planner

import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD

import ed.inf.lfcs.kgds.query._

class ProgramPlanner(spark: SparkSession){
  
  def execute(parseMap: Map[String, String]): RDD[Row] = {
    /* TEMPORARY LOGIC */
    
    // Define the schema for the columns
    val integerSchema = StructType(Seq(
      StructField("_c0", IntegerType, nullable = false),
      StructField("_c1", IntegerType, nullable = false),
      StructField("_c2", IntegerType, nullable = false)
    ))

    val stringSchema = StructType(Seq(
      StructField("_c0", StringType, nullable = false),
      StructField("_c1", StringType, nullable = false),
      StructField("_c2", StringType, nullable = false)
    ))

    println(s"$parseMap")

    val dumpFilePath = parseMap("dump")
    val dumpType = parseMap("dumptype")
    val program = parseMap("program")

    val ext = dumpFilePath.split("\\.").last
    val delimiter = ext match {
      case "csv" => ","
      case "tsv" => "\t"
      case "nt" => " "
      case _ => throw new UnsupportedOperationException(s"File extension is: $ext. Expected csv|tsv|nt")
    }

    println(s"DELIMITER: $delimiter")

    var df: DataFrame = null
    
    if (dumpType == "numerical"){
      println(s"READING WITH INTEGER SCHEMA")
      
      df = spark.read
        .format("csv")
        .option("delimiter", delimiter)
        .schema(integerSchema)
        .load(dumpFilePath)
    } else{
      println(s"READING WITH STRING SCHEMA")

      df = spark.read
        .format("csv")
        .option("delimiter", delimiter)
        .schema(stringSchema)
        .load(dumpFilePath)
    }
    
    println("DUMP READING DONE")
    
    if (program == "instance_of_subclass")
    {
      if (dumpType == "numerical"){
        val subclassPredicate = parseMap("subclass_of").toInt
        val seed = parseMap("seed").toInt
        val instancePredicate = parseMap("instance_of").toInt
        
        println("STARTING SUBSET EXTRACTION")
        
        val subclass_extractor: SimpleRecursiveQuery[Int] = new SimpleRecursiveQuery[Int]
        val subclass_results = subclass_extractor.getSubjectByObjectSeedRecOnPredicate(subclassPredicate, Set(seed), df)

        println(s"RECURSIVE FINISHED, LEN: ${subclass_results.size}")

        val item_extratcor:SimpleFilteringQuery[Int] = new SimpleFilteringQuery[Int]
        val item_results = item_extratcor.getSubjectByObjectSeedOnPredicate(instancePredicate, subclass_results, df)
        
        spark.sparkContext.parallelize(item_results.toSeq).map {
          case (intValue) => Row(intValue)
        }
      }
        else{ spark.sparkContext.emptyRDD}
    }
    else{ spark.sparkContext.emptyRDD}
  }
}