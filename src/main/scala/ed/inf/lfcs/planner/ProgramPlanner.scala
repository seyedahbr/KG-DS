package ed.inf.lfcs.kgds.planner

import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD

import ed.inf.lfcs.kgds.query._

class ProgramPlanner(spark: SparkSession){
  
  def execute(parseMap: Map[String, String]): DataFrame = {
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
    
    if (dumpType == "numerical") {
      println(s"READING WITH INTEGER SCHEMA")
      
      df = spark.read
        .format("csv")
        .option("delimiter", delimiter)
        .schema(integerSchema)
        .load(dumpFilePath)
    } else {
      println(s"READING WITH STRING SCHEMA")

      df = spark.read
        .format("csv")
        .option("delimiter", delimiter)
        .schema(stringSchema)
        .load(dumpFilePath)
    }
    
    println("DUMP READING DONE")
    
    import spark.implicits._

    if (program == "instance_of_subclass") {
      if (dumpType == "numerical"){
        val subclassPredicate = parseMap("subclass_of").toInt
        val seed = parseMap("seed").toInt
        val instancePredicate = parseMap("instance_of").toInt
        
        println("STARTING SUBSET EXTRACTION")
        
        val subclass_extractor: SimpleRecursiveQuery[Int] = new SimpleRecursiveQuery[Int]
        val subclass_results = subclass_extractor.getSubjectByObjectSeedRecOnPredicate(subclassPredicate, Set(seed), df)

        println(s"RECURSIVE FINISHED, LEN: ${subclass_results.size}")

        val item_extractor: SimpleFilteringQuery[Int] = new SimpleFilteringQuery[Int]
        val item_results = item_extractor.getSubjectByObjectSeedOnPredicate(instancePredicate, subclass_results, df)
        
        // spark.sparkContext.parallelize(item_results.toSeq).map {
        //   case (intValue) => Row(intValue)
        // }

        spark.sparkContext.parallelize(item_results.toSeq).toDF
        
      }
      else { 
        val subclassPredicate = parseMap("subclass_of")
        val seed = parseMap("seed")
        val instancePredicate = parseMap("instance_of")
        
        println("STARTING SUBSET EXTRACTION")
        
        val subclass_extractor: SimpleRecursiveQuery[String] = new SimpleRecursiveQuery[String]
        val subclass_results = subclass_extractor.getSubjectByObjectSeedRecOnPredicate(subclassPredicate, Set(seed), df)

        println(s"RECURSIVE FINISHED, LEN: ${subclass_results.size}")

        val item_extractor: SimpleFilteringQuery[String] = new SimpleFilteringQuery[String]
        val item_results = item_extractor.getSubjectByObjectSeedOnPredicate(instancePredicate, subclass_results, df)
        
        // spark.sparkContext.parallelize(item_results.toSeq).map {
        //   case (intValue) => Row(intValue)
        // }

        spark.sparkContext.parallelize(item_results.toSeq).toDF
      }
    }
    else if (program == "shortest_path") {
      if (dumpType == "numerical") {
        val src = parseMap("source").toInt
        val des = parseMap("destination").toInt

        println("STARTING SHORTEST PATH")

        val shpth: SimpleShortestPath[Int] = new SimpleShortestPath[Int]
        val shpthDF = shpth.getShortestPathSrcDes(df, src, des)
        
        shpthDF.show(truncate = false)

        val temp = Seq("TODO")
        spark.sparkContext.parallelize(temp).toDF

      }
      else { val temp = Seq("TODO")
        spark.sparkContext.parallelize(temp).toDF }
    }
    else if (program == "simple_triple_filter"){

      if (dumpType == "numerical") {
        val subjSeed = Set.empty[Int]
        val predSeed = Set(-280828914,-840342212)
        val objSeed = Set.empty[Int]
        val negFiltering = true

        val triple_extractor: SimpleFilteringQuery[Int] = new SimpleFilteringQuery[Int]
        val triple_results = if (negFiltering) {
          triple_extractor.getTriplesNotInSeed(subjSeed, predSeed, objSeed, df)
        } else {
          triple_extractor.getTriplesInSeed(subjSeed, predSeed, objSeed, df)
          null
        }
        triple_results
      }
      else {
        val subjSeed = Set.empty[String]
        val predSeed = Set.empty[String]
        val objSeed = Set.empty[String]
        val negFiltering = true

        val triple_extractor: SimpleFilteringQuery[String] = new SimpleFilteringQuery[String]
        val triple_results = if (negFiltering) {
          triple_extractor.getTriplesNotInSeed(subjSeed, predSeed, objSeed, df)
        } else {
          triple_extractor.getTriplesInSeed(subjSeed, predSeed, objSeed, df)
          null
        }
        triple_results
      }
    }
    else if (program == "remove_literals"){
      if (dumpType == "textual") {
        val literal_remover: SimpleFilteringQuery[String] = new SimpleFilteringQuery[String]
        literal_remover.getNonLiterals(df)
      }
      else { throw new UnsupportedOperationException("Program not found") }
    }
    else if (program == "wdt_to_items"){
      if (dumpType == "textual") {
        val literal_remover: SimpleFilteringQuery[String] = new SimpleFilteringQuery[String]
        literal_remover.getWdtToItems(df)
      }
      else { throw new UnsupportedOperationException("Program not found") }
    }
    else { throw new UnsupportedOperationException("Program not found") }
  }
}