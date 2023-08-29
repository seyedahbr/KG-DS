package ed.inf.lfcs.kgds.query

import org.apache.spark.sql.{DataFrame, Row, Encoder, Encoders}
import org.apache.spark.sql.functions._
import scala.reflect.ClassTag

class SimpleRecursiveQuery[T: ClassTag]{
  private var allObjects: Set[T] = Set.empty
  
  def getSubjectByObjectSeedRecOnPredicate(predicate: T, objectSeed: Set[T], df: DataFrame): Set[T] = {
    allObjects ++= objectSeed

    println(s"STARTING GETTING SUBJECTS BY OBJECT SEED RECURSIVELY ON PREDICATE - LEN SEED: ${allObjects.size}")
    
    getSubjectRecOnPredicate(predicate, allObjects, df)
  }
  
  private def getSubjectRecOnPredicate(predicate: T, objects: Set[T], df: DataFrame): Set[T] = {
    val lst = objects.toList

    println(s"LEN OBJECTS (before query): ${lst.length}")

    val queryObjects: List[T] = df
      .filter((col("_c1") === predicate ) && (col("_c2").isin(lst:_*)) )
      .select("_c0")
      .rdd
      .map(row => row.getAs[T](0))
      .collect()
      .toList
      .distinct
    
    println(s"LEN QUERIED OBJECTS: ${queryObjects.length}")

    val newObjects: Set[T] = queryObjects.toSet -- allObjects

    println(s"LEN NEW OBJECTS: ${newObjects.size}")

    if (newObjects.isEmpty) Set.empty[T] else{
      allObjects ++= newObjects

      println(s"LEN ALL RETRIEVEDS: ${allObjects.size}")

      newObjects ++ getSubjectRecOnPredicate(predicate, newObjects, df)
    }
  }
}
