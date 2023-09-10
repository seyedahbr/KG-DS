package ed.inf.lfcs.kgds.query

import org.apache.spark.sql.{DataFrame, Row, Encoder, Encoders}
import org.apache.spark.sql.functions._
import scala.reflect.ClassTag

class SimpleShortestPath[T: ClassTag]{
  
  private def expandPaths(df: DataFrame, pathsDF: DataFrame): DataFrame = {                                       
      val expandedPaths = pathsDF.join(df, element_at(pathsDF("path"), size(pathsDF("path"))) === df("_c0"))
        .withColumn("new_path", concat(pathsDF("path"), lit(" -> "), df("_c2")))
        .drop("path")
        .withColumnRenamed("new_path", "path")
      expandedPaths
    }
  
  def getRecShortestPathSrcDes(df: DataFrame, sourceEntity: T, targetEntity: T): DataFrame = {
    var pathsDF = df.filter(col("_c0") === sourceEntity)
      .withColumn("path", array(col("_c0")))

    // while (pathsDF.filter(element_at(col("path"), size(col("path"))) === targetEntity)) {
    //   pathsDF = expandPaths(df, pathsDF)
    // }

    pathsDF.filter(col("path").getItem(size(col("path")) - 1) === targetEntity)
  }

  def getShortestPathSrcDes(df: DataFrame, sourceEntity: T, targetEntity: T): DataFrame = {
    val initialPathsDF = df.filter(col("_c0") === sourceEntity)
      .select(col("_c0"), col("_c1"), col("_c2"), array(lit(sourceEntity)).as("path"))
    
    var pathsDF = initialPathsDF

    while (pathsDF.filter(col("_c0") === targetEntity).isEmpty) {
      val expandedPathsDF = pathsDF.join(df, pathsDF("_c2") === df("_c0"))
        .select(
          df("_c2").as("_c0"),
          df("_c1"),
          df("_c2"),
          concat(pathsDF("path"), lit(" -> "), df("_c1"), lit(" -> "), df("_c2")).as("path")
        )

      pathsDF = expandedPathsDF
    }

    pathsDF.filter(col("_c0") === targetEntity)
  }
}
