/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.spark

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row}

import scala.language.implicitConversions

/**
  * @author yliu29.
  *
  * Extra functions available on DataFrame through an implicit conversion.
  */
class DataFrameFunctions(df: DataFrame) {

  /**
    * Drop duplicate columns when want to join `this` and `other`.
    * `this` and `other` may contain same columns and this function drops the duplicate
    * columns from `this`, notice that the columns used to join on are kept.
    *
    * @param other the `other` dataframe
    * @param joinColumns the columns used to join on
    * @return `this` dataframe which already drops the duplicate columns.
    */
  def dropDuplicateColumns(other: DataFrame, joinColumns: String*): DataFrame = {
    val set = (other.columns.toBuffer -- joinColumns).toSet
    df.drop(df.columns.filter(set): _*)
  }

  /**
    * Rename some columns of dataframe
    *
    * @param aliases the aliases of columns
    * @param reverse if reverse the column names by aliases
    * @return the dataframe after columns renamed.
    */
  def withColumnsRenamed(aliases: Array[(String, String)], reverse: Boolean = false): DataFrame = {
    var res = df
    if (aliases != null) {
      if (reverse == false) {
        aliases.foreach(e => {
          res = res.withColumnRenamed(e._1, e._2)
        })
      } else {
        aliases.foreach(e => {
          res = res.withColumnRenamed(e._2, e._1)
        })
      }
    }
    res
  }

  /**
    * zipWithIndex for DataFrame
    *
    * @param colName the index column name
    * @param offset the start offset
    * @param inFront whether the index column is in front
    * @return
    */
  def zipWithIndex(colName: String = "id",
                   offset: Int = 0,
                   inFront: Boolean = true) : DataFrame = {
    df.sqlContext.createDataFrame(
      df.rdd.zipWithIndex.map(ri =>
        Row.fromSeq(
          (if (inFront) Seq(ri._2 + offset) else Seq())
            ++ ri._1.toSeq ++
            (if (inFront) Seq() else Seq(ri._2 + offset))
        )
      ),
      StructType(
        (if (inFront) Array(StructField(colName, LongType, false)) else Array[StructField]())
          ++ df.schema.fields ++
          (if (inFront) Array[StructField]() else Array(StructField(colName, LongType, false)))
      )
    )
  }

  /**
    * A special flatmap, it flats one row to number of rows where the number is
    * value of `nCol`.
    *
    * @param nCol the column whose value is for the flat
    * @return
    */
  def flatMap(nCol: String) : DataFrame = {
    df.sqlContext.createDataFrame(
      df.rdd.flatMap(row => {
        val n = row.getAs[Int](nCol)
        new Iterator[Row] () {
          var i = 0
          override def hasNext: Boolean = i < n
          override def next(): Row = {
            i = i + 1
            row
          }
        }
      }),
      StructType(df.schema.fields)
    )
  }

  /**
    * Save the dataframe on HDFS
    *
    * @param job the spark job
    * @param path the path to save to
    * @param format output format
    * @param compress the compress format
    * @return
    */
  def save(job: BaseSparkJob, path: String,
           format: String = "parquet", compress: String = null) : DataFrame = {
    cache(job, path, format, compress)
  }

  /**
    * *Cache* the dataframe on HDFS. In ebay Hadoop env, saving
    * on HDFS is more efficient than using Spark cache.
    *
    * @param job the spark job
    * @param path the path to save to
    * @param format output format
    * @param compress the compress format
    * @return
    */
  def cache(job: BaseSparkJob, path: String,
            format: String = "parquet", compress: String = "snappy") : DataFrame = {
    val tmpPath = (new Path(path)).toString + "tmp"
    job.saveDFToFiles(df, tmpPath, outputFormat = format, compressFormat = compress)
    val fs = job.fs
    fs.delete(new Path(path), true)
    fs.rename(new Path(tmpPath), new Path(path))
    job.readFilesAsDF(path, inputFormat = format)
  }

  /**
    * Fill nullable column with reference dataframe iff the column in df is null
    *
    * @param dfReference reference dataframe used to fill the value
    * @param nullColumn nullable column
    * @param joinColumns join columns between df and dfReference
    * @return
    */
  def fillColumnIfNull(dfReference: DataFrame, nullColumn: String, joinColumns: Seq[String]): DataFrame = {
    // TODO: use UDF to check value in left or right
    val df1 = df.filter(col(nullColumn).isNotNull)
    val df2 = df.filter(col(nullColumn).isNull)
      .drop(nullColumn)
      .join(dfReference.distinct(), joinColumns, "left_outer")
      .select(df1.columns.map(col(_)): _*)

    if (df2 != null) {
      if (df1 == null) df2 else df1.union(df2)
    } else df1
  }

  /**
    * Join with nullable column
    *
    * @param rightDF reference dataframe used to fill the value
    * @param columns nullable column
    * @param joinType join columns between df and dfReference
    * @return
    */
  def joinWithNullableColumn(rightDF: DataFrame, columns: Seq[String], joinType: String): DataFrame = {
    // NULL safe equality operator: if two columns are null
    val colExpr: Column = df(columns.head) <=> rightDF(columns.head)
    val fullExpr = columns.tail.foldLeft(colExpr) {
      (colExpr, p) => colExpr && df(p) <=> rightDF(p)
    }
    var dfRet = df.join(rightDF, fullExpr, joinType)

    columns.foreach(col => {
      dfRet = dfRet.drop(rightDF.col(col))
    })

    dfRet
  }


}

object DataFrameFunctions {

  /** Implicit conversion from an DataFrame to DataFrameFunctions. */
  implicit def fromDataFrame(df: DataFrame): DataFrameFunctions = {
    new DataFrameFunctions(df)
  }
}
