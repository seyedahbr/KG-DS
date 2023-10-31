package ed.inf.lfcs.kgds.planner

import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD

import ed.inf.lfcs.kgds.query._

class ProgramPlanner(spark: SparkSession){
  
  def execute(parseMap: Map[String, String]): DataFrame = {
    /* TEMPORARY LOGIC */
    
    // Define the schema for the columns
    val stringSchema = StructType(Seq(
      StructField("_c0", StringType, nullable = false),
      StructField("_c1", StringType, nullable = false),
      StructField("_c2", StringType, nullable = false)
    ))

    println(s"$parseMap")

    val dumpFilePath = parseMap("dump")
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
    println(s"READING WITH STRING SCHEMA")

    df = spark.read
      .format("csv")
      .option("delimiter", delimiter)
      .schema(stringSchema)
      .load(dumpFilePath)
    
    println("DUMP READING DONE")
    
    import spark.implicits._

    if (program == "instance_of_subclass") {    
      val subclassPredicate = parseMap("subclass_of")
      val seed = parseMap("seed")
      val instancePredicate = parseMap("instance_of")
      
      println("STARTING SUBSET EXTRACTION")
      
      val subclass_extractor: SimpleRecursiveQuery[String] = new SimpleRecursiveQuery[String]
      val subclass_results = subclass_extractor.getSubjectByObjectSeedRecOnPredicate(subclassPredicate, Set(seed), df)

      println(s"RECURSIVE FINISHED, LEN: ${subclass_results.size}")

      val item_extractor: SimpleFilteringQuery[String] = new SimpleFilteringQuery[String]
      val item_results = item_extractor.getSubjectByObjectSeedOnPredicate(instancePredicate, subclass_results, df)
      spark.sparkContext.parallelize(item_results.toSeq).toDF
      
    }
    else if (program == "shortest_path") {
      val src = parseMap("source")
      val des = parseMap("destination")

      println("STARTING SHORTEST PATH")

      val shpth: SimpleShortestPath[String] = new SimpleShortestPath[String]
      val shpthDF = shpth.getShortestPathSrcDes(df, src, des)
      
      shpthDF.show(truncate = false)

      val temp = Seq("TODO")
      spark.sparkContext.parallelize(temp).toDF   
    }
    else if (program == "simple_triple_filter"){
      val subj = parseMap.getOrElse("subject", "ANY")
      val subjSeed = Option(subj)
        .filter(_ != "ANY")
        .map(_.stripPrefix("NOTIN").replaceAll("[{}]", "").split(",").map(_.trim).toSet)
        .getOrElse(Set.empty)
      val subjNeg = subj.startsWith("NOTIN")
      
      val pred = parseMap.getOrElse("predicate", "ANY")
      val predSeed = Option(pred)
        .filter(_ != "ANY")
        .map(_.stripPrefix("NOTIN").replaceAll("[{}]", "").split(",").map(_.trim).toSet)
        .getOrElse(Set.empty)
      val predNeg = subj.startsWith("NOTIN")
      
      val obj = parseMap.getOrElse("object", "ANY")
      val objSeed = Option(obj)
        .filter(_ != "ANY")
        .map(_.stripPrefix("NOTIN").replaceAll("[{}]", "").split(",").map(_.trim).toSet)
        .getOrElse(Set.empty)
      val objNeg = subj.startsWith("NOTIN") 

      val triple_extractor: SimpleFilteringQuery[String] = new SimpleFilteringQuery[String]
      triple_extractor.getTriplesInSeed(subjSeed, subjNeg, predSeed, predNeg, objSeed, objNeg, df)
    }
    else if (program == "remove_literals"){
      val literal_remover: HierarchicalFiltering[String] = new HierarchicalFiltering[String]
      literal_remover.getNonLiterals(df)      
    }
    else if (program == "remove_externals"){
      val internalKGIRIPrefix = parseMap("internal_iri_prefix")
      val external_IRI_remover: HierarchicalFiltering[String] = new HierarchicalFiltering[String]
      external_IRI_remover.getInternalIRIObjects(internalKGIRIPrefix, df)
    }
    else if (program == "remove_non_item_contextuals"){
      val statementIRIPrefix = parseMap("statement_iri_prefix")
      val itemIRIPrefix = parseMap("item_iri_prefix")
      val referenceIRIPrefix = parseMap("reference_iri_prefix")
      val itemGraphExtractor: HierarchicalFiltering[String] = new HierarchicalFiltering[String]
      itemGraphExtractor.getItemsGraph(statementIRIPrefix, itemIRIPrefix, referenceIRIPrefix, df)
    }
    else if (program == "fact_to_items"){
      val itemIRIPrefix = parseMap("item_iri_prefix")
      val factIRIPrefix = parseMap("fact_iri_prefix")
      val literal_remover: HierarchicalFiltering[String] = new HierarchicalFiltering[String]
      literal_remover.getFactToItems(itemIRIPrefix, factIRIPrefix, df)
    }
    else { throw new UnsupportedOperationException("Program not found") }
  }
}