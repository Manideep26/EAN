package com.thomsonreuters.ecp.cdap.spark

import com.thomsonreuters.ecp.AppHelper
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Created by U6039656 on 10/25/2017.
  */
/*
 * Make a Hive table (if needed) and point it at the given Parquet file.
 */
object ParquetToHive extends App {
  val log = LoggerFactory.getLogger(ParquetToHive.getClass)

  val sparkConf = new SparkConf().setAppName("ParquetToHive")
  val sc = new SparkContext(sparkConf)
  val hiveContext = new HiveContext(sc)

  val hdfs = AppHelper.makeHdfsFileSystem()
  val appConfig = AppHelper.makeAppConfig()

  val TABLE_HDFS_LOCATION = appConfig.getString("hiveSourceFilename")
  val DATABASE_NAME = appConfig.getString("clusterNamespace")
  val TABLE_NAME = "eantickerdata"

  createTable(TABLE_HDFS_LOCATION)
  updateTableLocation(TABLE_HDFS_LOCATION)

  def createDatabase() = {
    hiveContext.sql(
      s"""CREATE DATABASE IF NOT EXISTS
        |  $DATABASE_NAME
        |  COMMENT 'EAN Ticker Data'
        |""".stripMargin)
  }

  /*
  The Parquet file is currently:
message spark_schema {
  optional int64 OrganizationID;
  optional int64 InstrumentID;
  optional int64 QuoteID;
  optional binary TickerSymbol (UTF8);
  optional binary MIC (UTF8);
  optional int64 OrgMainQuoteId;
  optional int64 InstrumentMainQuoteId;
  optional binary AssetState (UTF8);
  optional binary SourceName (UTF8);
}

   */

  class InvalidHivePathException(path : String, problem : String)
  extends Exception(s"This HDFS path is not valid for Hive because it $problem: $path")

  /**
    * Ensure:
    *
    * 1. The path does not contain any ' symbols (No SQL Injection for you!)
    * 2. The path exists in HDFS
    *
    * @param path A path on HDFS, like /project/ecpdeveantoneptune/EANFiles/EANTICKERDATA.parquet
    * @return true if the path is good, false otherwise.
    */
  def throwIfInvalidPath(path : String): Unit = {
    if (path.contains("'")) {
      throw new InvalidHivePathException(path, "must not contain single-quotes (')")
    }
    val hdfsPath = new org.apache.hadoop.fs.Path(path)
    val exists = hdfs.exists(hdfsPath)
    if (! exists) {
      throw new InvalidHivePathException(path, "does not exist on HDFS")
    }
  }

  def createTable(path : String = TABLE_HDFS_LOCATION) = {
    throwIfInvalidPath(path)
    createDatabase()
    hiveContext.sql(
      s"""
        |CREATE EXTERNAL TABLE IF NOT EXISTS $DATABASE_NAME.$TABLE_NAME (
        |  OrganizationID         BIGINT,
        |  InstrumentID           BIGINT,
        |  QuoteID                BIGINT,
        |  TickerSymbol           STRING,
        |  MIC                    STRING,
        |  OrgMainQuoteId         BIGINT,
        |  InstrumentMainQuoteId  BIGINT,
        |  AssetState             STRING,
        |  SourceName             STRING
        |  )
        |  COMMENT 'EAN Ticker Data'
        |  STORED AS parquet
        |  LOCATION '$path'
        | """.stripMargin)
  }

  def updateTableLocation(newPath : String = TABLE_HDFS_LOCATION) = {
    throwIfInvalidPath(newPath)
    hiveContext.sql(
      s"""
          |ALTER TABLE $DATABASE_NAME.$TABLE_NAME
          |SET LOCATION '$newPath'
      """.stripMargin)
  }

  def dropTable() = {
    hiveContext.sql(s"DROP TABLE $DATABASE_NAME.$TABLE_NAME")
  }

  def replaceTable(path : String = TABLE_HDFS_LOCATION) = {
    throwIfInvalidPath(path)
    dropTable()
    createTable(path)
  }

}
