package com.unity.analytics.spark.utils.parquet

import java.io.File
import java.sql.{Connection, DriverManager}
import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path}
import org.apache.spark.Logging
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.collection.mutable.ListBuffer

/**
  * Uploads data from parquet files on HDFS to MySQL.
  * For fast upload we do the following:
  * 1) Batch into target number of CSV files
  * 2) Provide hooks for sorting data based on primary key
  * 3) Use LOAD command to upload batch CSVs
  * 4) Parallel number of connections to MySQL
  *
  * Should be able to make this work with S3 using S3FileSystem.
  *
  * @param sourceDir           parquet directory
  * @param destinationDir      destination directory of where to store the CSVs.
  * @param targetNumberOfCsv   max number of CSV files to generate. It is possible less number of files will be produced.
  * @param parallelConnections the parallel number of connections to execute LOAD command to MySQL
  * @param table               implementation of the MySqlTable trait. This impl is specific to a MySQL table
  * @param jdbcUrl             the JDBC URL for mysql that includes the credentials e.g. jdbc:mysql://hostname:port/db_schema?user=name&password=password
  * @param sqlContext          the sparkSQL context
  */
class MySqlUploader(
                     sourceDir: String,
                     destinationDir: String,
                     targetNumberOfCsv: Int,
                     parallelConnections: Int,
                     table: MySqlTable,
                     jdbcUrl: String,
                     sqlContext: SQLContext)
  extends Logging with Serializable {

  def uploadFromCsv = {

    convertToCsv
    val sparkContext = sqlContext.sparkContext
    val fs = FileSystem.get(sparkContext.hadoopConfiguration)
    val tablePath = new Path(destinationDir)
    var list = ListBuffer[LocatedFileStatus]()
    if (fs.exists(tablePath)) {
      val iter = fs.listFiles(tablePath, true)
      while (iter.hasNext) {
        list += iter.next()
      }
    }

    // need to filter out the meta-data files
    val pathList = list
      .filter(status => status.isFile && status.getPath.toString.contains("part-"))
      .map(_.getPath.toString)

    val tableName = table.getTableName
    val targetParallelism = math.min(parallelConnections, pathList.length)
    logInfo(s"Start uploading to table=${tableName} ${pathList.size} files using ${targetParallelism} tasks")

    sparkContext.parallelize(pathList, targetParallelism).foreachPartition(
      iter => {
        val fs = FileSystem.get(new Configuration())
        val connection = DriverManager.getConnection(jdbcUrl)

        val localFile: File = null
        try {
          iter.foreach(path => {
            logInfo(s"Start downloading file=${path}")
            val localFile = File.createTempFile(tableName, UUID.randomUUID().toString)
            fs.copyToLocalFile(new Path(path), new Path(localFile.getAbsolutePath))
            uploadFileToMySQL(localFile, connection)
          })
        }
        finally {
          if (localFile != null) {
            localFile.delete()
          }

          connection.close()
        }
      }

    )

    logInfo(s"Completed uploading to table=${tableName}")
  }

  private def loadDataFrame: DataFrame = {
    var reader = sqlContext.read
    val schema = table.getSchema
    if (schema.isDefined) {
      reader = reader.schema(schema.get)
    }

    reader.parquet(sourceDir)
  }

  private def convertToCsv: Unit = {
    // Sorting the dataset is preferred as it keeps the upload data key space separate when inserting
    // multiple files in parallel. There is an issue with using sort with DF. Spark changes the partition number after sort()
    // thus we cannot repartition before processDataFrame(). However, repartition after processDataFrame()
    // will shuffle and lose the ordering.
    // We use a best effort approach where we coalesce after processDataFrame(). coalesce() does not
    // shuffle thus preserve partial ordering. The limitation is that coalesce only works to reduce partitions (batch)
    // and not for increasing partitions (split). Thus targetNumberOfCsv serve as a max target number.
    // For splitting, it may be more straightforward to use direct file operation instead (not supported).
    var df = loadDataFrame
    table.processDataFrame(df)
      .coalesce(targetNumberOfCsv)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "false")
      // need to force quote for all values. The reason why we need this is because for
      // some tables we use the ENCLOSED BY option in the LOAD INFILE command for the JSON
      // blob that requires escaping. When this option is used, mysql will interpret NULL
      // string without double quotes to be null value. To eliminate ambiguity of null value
      // vs NULL string value, we force quote on all values.
      .option("quote", "\"")
      .option("quoteMode", "ALL")
      .mode(SaveMode.Overwrite)
      .save(destinationDir)
  }

  private def uploadFileToMySQL(file: File, connection: Connection) = {
    val sql = table.getLoadDataStatement(file.getAbsolutePath)
    logInfo(s"Uploading file=${file.getAbsolutePath} with size=${file.length()} to table=${table.getTableName}. SQL=${sql}")
    connection.prepareStatement(sql).execute()
    file.delete()
  }
}
