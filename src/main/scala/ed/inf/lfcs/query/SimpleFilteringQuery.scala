package ed.inf.lfcs.kgds.query

import org.apache.spark.sql.{DataFrame, Row, Encoder, Encoders}
import org.apache.spark.sql.functions._
import scala.reflect.ClassTag

class SimpleFilteringQuery[T: ClassTag]{
  
  def getSubjectByObjectSeedOnPredicate(predicate: T, objectSeed: Set[T], df: DataFrame): Set[T] = {
    println(s"STARTING GETTING SUBJECTS BY OBJECT SEED ON PREDICATE - LEN SEED: ${objectSeed.size}")
    
    val lst = objectSeed.toList
    val queryObjects: List[T] = df
      .filter((col("_c1") === predicate ) && (col("_c2").isin(lst:_*)) )
      .select("_c0")
      .rdd
      .map(row => row.getAs[T](0))
      .collect()
      .toList
      .distinct
    
    println(s"LEN QUERIED OBJECTS: ${queryObjects.length}")
    
    return queryObjects.toSet
  }
}
