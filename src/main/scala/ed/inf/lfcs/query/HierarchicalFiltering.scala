package ed.inf.lfcs.kgds.query

import org.apache.spark.sql.{DataFrame, Row, Encoder, Encoders}
import org.apache.spark.sql.functions._
import scala.reflect.ClassTag

class HierarchicalFiltering{

  def getLiteralsNoLabelling(labelingPredicates: Set[String], df: DataFrame): DataFrame = {
    println("STARTING EXTRACTING LITERALS")

    val labellingPredLst = labelingPredicates.toList
    val condition = !col("_c2").startsWith("<") && !col("_c1").isin(labellingPredLst:_*)
    df.filter(condition)
      .select("_c0", "_c1", "_c2")
  }

  def getExternalIRIs(internalIRIPrefix: String, df: DataFrame): DataFrame = {
    println("STARTING EXTRACTING EXTERNAL IRIS")
    
    val condition = col("_c2").startsWith("<") && !col("_c2").startsWith("<" + internalIRIPrefix)
    df.filter(condition)
      .select("_c0", "_c1", "_c2")
  }
  
  def getNonLiterals(df: DataFrame): DataFrame = {
    println("STARTING REMOVING LITERALS")
    
    val condition = col("_c2").startsWith("<")
    df.filter(condition)
      .select("_c0", "_c1", "_c2")
  }

  def getInternalIRIObjects(internalIRIPrefix: String, df: DataFrame): DataFrame = {
    println("STARTING REMOVING EXTERNAL IRIS")
    
    val condition = col("_c2").startsWith("<" + internalIRIPrefix)
    df.filter(condition)
      .select("_c0", "_c1", "_c2")
  }

  // TODO: Needed to be changed (driver memory exceed)
  def getItemsGraph(statementIRIPrefix: String, itemIRIPrefix: String, referenceIRIPrefix: String, df: DataFrame): DataFrame = {
      
    val baseDf = getNonLiterals(df)

    println("STARTING FILTERING STATEMENT->ITEM TRIPLES")

    val qualifiersCondition = col("_c0").startsWith(statementIRIPrefix) &&
    col("_c2").startsWith(itemIRIPrefix)
    val qualStatementsList = baseDf.filter(qualifiersCondition)
      .select("_c0")
      .rdd
      .map(row => row.getAs[String](0))
      .collect()
      .toList
      .distinct
    
    println("STARTING FILTERING REFERENCE->ITEM TRIPLES")
    val referencesCondition = col("_c0").startsWith(referenceIRIPrefix) &&
    col("_c2").startsWith(itemIRIPrefix)
    val refList = baseDf.filter(referencesCondition)
      .select("_c0")
      .rdd
      .map(row => row.getAs[String](0))
      .collect()
      .toList
      .distinct
    
    println("STARTING FILTERING STATEMENT->REFERENCE->ITEM TRIPLES")
    val refStatementCondition = col("_c0").startsWith(statementIRIPrefix) &&
    col("_c2").isin(refList:_*)
    val refStatementList = baseDf.filter(refStatementCondition)
      .select("_c0")
      .rdd
      .map(row => row.getAs[String](0))
      .collect()
      .toList
      .distinct
    
    val mergedList = (qualStatementsList ++ refStatementList).toSet.toList

    println("STARTING FILTERING ITEMS GRAPH")
    val condition = !(col("_c0").startsWith(statementIRIPrefix) || 
    col("_c0").startsWith(referenceIRIPrefix)) ||
    (col("_c0").startsWith(statementIRIPrefix) && col("_c0").isin(mergedList:_*)) ||
    (col("_c0").startsWith(referenceIRIPrefix) && col("_c0").isin(refList:_*))
    
    baseDf.filter(condition)
      .select("_c0", "_c1", "_c2")
  }

  def getFactToItems(factIRIPrefix: String, itemIRIPrefix: String, df: DataFrame): DataFrame = {
    println("STARTING FILTERING FACT:->ITEMS TRIPLES")
    
    val condition = (col("_c1").startsWith(factIRIPrefix)) &&
    (col("_c2").startsWith(itemIRIPrefix))

    df.filter(condition)
      .select("_c0", "_c1", "_c2")
  }
}
