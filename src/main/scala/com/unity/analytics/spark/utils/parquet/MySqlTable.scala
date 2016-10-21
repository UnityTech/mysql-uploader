package com.unity.analytics.spark.utils.parquet

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

/**
  * Interface that must be implemented by concrete classes
  */
trait MySqlTable {

  // MySQL table name
  def getTableName: String

  // optional: used for enforcing schema when reading from parquet
  def getSchema: Option[StructType] = {
    None
  }

  /**
    * Opportunity to pre-process the DateFrame.
    * TODO by derived class:
    * 1) Sort by primary key (more efficient loading) e.g.  df.sort(col("column_name"))
    * 2) Drop columns that are not needed e.g. df.drop("column_name")
    * 3) Rename columns to correspond with MySQL table column names e.g. df.withColumnRenamed("old_name", "new_name")
    * 4) Reorder order of fields to correspond in exact order of table fields in MySQL e.g. df.select("colx,coly")
    * 5) Converting data to the correct type used in MySQL e.g. df.withColumn("column_name", col("column_name").cast(DoubleType))
    */
  def processDataFrame(df: DataFrame): DataFrame = {
    df
  }


  /**
    * Generates the LOAD command specific to MySQL table
    */

  /* Example of LOAD command:
   s"LOAD DATA LOCAL INFILE '${filePath}' INTO TABLE `${getTableName}`" +
     " FIELDS TERMINATED BY ',' " +
     " ENCLOSED BY '\"' " +
     " ESCAPED BY '\"' " +
     " LINES TERMINATED BY '\n'" +
     " (col1, col2, col3) "
     */
  def getLoadDataStatement(filePath: String): String
}
