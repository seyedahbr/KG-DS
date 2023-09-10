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
}
