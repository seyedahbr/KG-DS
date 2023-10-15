package ed.inf.lfcs.kgds.query

import org.apache.spark.sql.{DataFrame, Row, Encoder, Encoders}
import org.apache.spark.sql.functions._
import scala.reflect.ClassTag

class SimpleFilteringQuery[T: ClassTag]{
  
  def getSubjectByObjectSeedOnPredicate(predicate: T, objectSeed: Set[T], df: DataFrame): Set[T] = {
    println(s"STARTING GETTING SUBJECTS BY OBJECT SEED ON PREDICATE - LEN SEED: ${objectSeed.size}")
    
    val lst = objectSeed.toList
    val queryObjects: List[T] = df
      .filter((col("_c1") === predicate ) && (col("_c2").isin(lst:_*)))
      .select("_c0")
      .rdd
      .map(row => row.getAs[T](0))
      .collect()
      .toList
      .distinct
    
    println(s"LEN QUERIED OBJECTS: ${queryObjects.length}")
    
    queryObjects.toSet
  }

  def getTriplesNotInSeed(subjectSeed: Set[T], predicateSeed: Set[T], objectSeed: Set[T], df: DataFrame): DataFrame = {
    println("STARTING FILTERING TRIPLES BY SEED")
    
    val subjlst = subjectSeed.toList
    val predlst = predicateSeed.toList
    val objlst = objectSeed.toList

    val condition = (!col("_c0").isin(subjlst:_*)) &&
      (!col("_c1").isin(predlst:_*)) &&
      (!col("_c2").isin(objlst:_*))

    df.filter(condition)
      .select("_c0", "_c1", "_c2")
  }

  def getTriplesInSeed(subjectSeed: Set[T], predicateSeed: Set[T], objectSeed: Set[T], df: DataFrame): DataFrame = {
    println("STARTING FILTERING TRIPLES BY SEED")
    
    val subjlst = subjectSeed.toList
    val predlst = predicateSeed.toList
    val objlst = objectSeed.toList

    val condition = (col("_c0").isin(subjlst:_*)) &&
      (col("_c1").isin(predlst:_*)) &&
      (col("_c2").isin(objlst:_*))

    df.filter(condition)
      .select("_c0", "_c1", "_c2")
  }

  def getNonLiterals(df: DataFrame): DataFrame = {
    println("STARTING REMOVING LITERALS")
    
    val condition = col("_c2").startsWith("<")
    df.filter(condition)
      .select("_c0", "_c1", "_c2")
  }

  def getInternalIRIObjects(df: DataFrame): DataFrame = {
    println("STARTING REMOVING EXTERNAL IRIS")
    
    val condition = col("_c2").startsWith("<http://www.wikidata.org/")
    df.filter(condition)
      .select("_c0", "_c1", "_c2")
  }

  def getItemsGraph(df: DataFrame): DataFrame = {
      
    val baseDf = getNonLiterals(df)

    println("STARTING FILTERING STATEMENT->ITEM TRIPLES")

    val qualifiersCondition = col("_c0").startsWith("<http://www.wikidata.org/entity/statement/") &&
    col("_c2").startsWith("<http://www.wikidata.org/entity/Q")
    val qualStatementsList = baseDf.filter(qualifiersCondition)
      .select("_c0")
      .rdd
      .map(row => row.getAs[T](0))
      .collect()
      .toList
      .distinct
    
    println("STARTING FILTERING REFERENCE->ITEM TRIPLES")
    val referencesCondition = col("_c0").startsWith("<http://www.wikidata.org/reference/") &&
    col("_c2").startsWith("<http://www.wikidata.org/entity/Q")
    val refList = baseDf.filter(referencesCondition)
      .select("_c0")
      .rdd
      .map(row => row.getAs[T](0))
      .collect()
      .toList
      .distinct
    
    println("STARTING FILTERING STATEMENT->REFERENCE->ITEM TRIPLES")
    val refStatementCondition = col("_c0").startsWith("<http://www.wikidata.org/entity/statement/") &&
    col("_c2").isin(refList:_*)
    val refStatementList = baseDf.filter(refStatementCondition)
      .select("_c0")
      .rdd
      .map(row => row.getAs[T](0))
      .collect()
      .toList
      .distinct
    
    val mergedList = (qualStatementsList ++ refStatementList).toSet.toList

    println("STARTING FILTERING ITEMS GRAPH")
    val condition = !(col("_c0").startsWith("<http://www.wikidata.org/entity/statement/") || 
    col("_c0").startsWith("<http://www.wikidata.org/reference/")) ||
    (col("_c0").startsWith("<http://www.wikidata.org/entity/statement/") && col("_c0").isin(mergedList:_*)) ||
     (col("_c0").startsWith("<http://www.wikidata.org/reference/") && col("_c0").isin(refList:_*))
    
    baseDf.filter(condition)
      .select("_c0", "_c1", "_c2")
  }

  def getWdtToItems(df: DataFrame): DataFrame = {
    println("STARTING FILTERING WDT:->ITEMS TRIPLES")
    
    val condition = (col("_c1").startsWith("<http://www.wikidata.org/entity/assert/P")) &&
    (col("_c2").startsWith("<http://www.wikidata.org/entity/Q"))

    df.filter(condition)
      .select("_c0", "_c1", "_c2")
  }
}
