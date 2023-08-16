package ed.inf.lfcs.kgds.runner

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

import ed.inf.lfcs.kgds.query._

object Runner{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkRecursiveDumpFiltering")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println("STARTING")

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

    
    val dumpFilePath = args(0)
    val dumpType = args(1)
    val seed = args(2).toInt

    println(s"DUMP: $dumpFilePath")

    val ext = dumpFilePath.split("\\.").last
    val delimiter = ext match {
      case "csv" => ","
      case "tsv" => "\t"
      case "nt" => " "
      case _ => throw new UnsupportedOperationException(s"File extension is: $ext. Expected csv|tsv|nt")
    }

    println(s"DELIMITER FOUND: $delimiter")


    val startTime = System.currentTimeMillis()
    var df: DataFrame = null
    
    
    if (dumpType == "num"){
      println(s"READING WITH INTEGER SCHEMA")
      
      df = spark.read
        .format("csv")
        .option("delimiter", delimiter)
        .schema(integerSchema)
        .load(dumpFilePath)
      
      val extractor: RecursiveQuery[Int] = new RecursiveQuery[Int]
      extractor.getSubjectByObjectSeedRecOnPredicate(-840342212, Set(-193471730), df)
    }
    else{
      println(s"READING WITH STRING SCHEMA")

      df = spark.read
        .format("csv")
        .option("delimiter", delimiter)
        .schema(stringSchema)
        .load(dumpFilePath)
    }    

    println("DUMP READING DONE")
    
    println("STARTING SUBSET EXTRACTION")

    // val extractor = if (dumpType == "num") {
      
    // } else {
    //   new RecursiveQuery[String]
    // }
    
    
  //   val qidRetriever = new QidRetriever(df)

  //   if (dumpType == "num-2023"){
  //     val subclasssList = qidRetriever.getQidsRecursiveNumerical2023(Set(seed), df)
  //     println("RECURSIVE FINISHED")
  //     println(s"LEN RESULT: ${subclasssList.size}")
  //     println("DONE")
  //     spark.stop()
  //   }

  //   else if (dumpType == "num-2015"){
  //     val subclasssList = qidRetriever.getQidsRecursiveNumerical2015(Set(seed), df)
  //     println("RECURSIVE FINISHED")
  //     println(s"LEN RESULT: ${subclasssList.size}")
  //     println("DONE")
  //     spark.stop()
  //   }

  //   else
  //     throw new UnsupportedOperationException(s"Undefined file type.")
  // }
  
    spark.stop()
  }
}
