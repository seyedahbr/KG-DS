package ed.inf.lfcs.kgds.query

import org.apache.spark.sql.{DataFrame, Row, Encoder, Encoders}
import org.apache.spark.sql.functions._
import scala.reflect.ClassTag

class HierarchicalFiltering[T: ClassTag]{
  
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

  // TODO: Needed to be changed (driver memory exceed)
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
