package com.thomsonreuters.ecp.cdap.spark

import java.text.SimpleDateFormat
import java.util.Calendar

import com.thomsonreuters.ecp.AppHelper
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Created by U6039656 on 10/25/2017.
  */
object BCPToParquet extends App {
  // table first into a data frame.
  // make sure to use com.databricks:spark-csv version 1.3+
  // which has consistent treatment of empty strings as nulls.
  val log = LoggerFactory.getLogger(BCPToParquet.getClass)
  val EANQUOTE = "EANQUOTE"
  val EANORGANIZATION = "EANORGANIZATION"
  val EANINSTRUMENT = "EANINSTRUMENT"
  val EANFMMSOURCENAME = "EANFMMSOURCENAME"
  val EANFMMIDENTIFIER = "EANFMMIDENTIFIER"
  val EANTICKERDATA = "EANTICKERDATA"

  val conf = new SparkConf().setAppName("BCPToParquet")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val hdfs = AppHelper.makeHdfsFileSystem()

  val confFactory = AppHelper.makeAppConfig()
  val workingDir = new Path(confFactory.getString("workingDirectory"))

  val DATE_FORMATTER = new SimpleDateFormat("yyyyMMdd-HHmmss")
  val timestamp = DATE_FORMATTER.format(Calendar.getInstance.getTime)
  val outputFilename = s"$workingDir/$EANTICKERDATA-$timestamp.parquet"

  val dfs = for {
    arg <- args
    filename = convertBCPToCsv(arg)
  }  yield (arg, sqlContext.read
    .format("com.databricks.spark.csv")
    .option("inferSchema", "true")
    .option("delimiter", "\u0001")
    .option("header", "true")
    .option("nullValue", "")
    .option("treatEmptyValuesAsNulls", "true")
    .load(new Path(workingDir, filename).toString()))

  for {
    (name, df) <- dfs
  } df.registerTempTable(name.replaceAll("[0-9]","").replaceAll("_","").dropRight(4))

  def convertBCPToCsv(arg:String): String = {
    log.info(arg.replaceAll("[0-9]","").replaceAll("_","").dropRight(4))
    val file = sc.textFile(new Path(workingDir, arg).toString())
    val newFileString = file.map(x => x.replace("|^|", "\u0001").replace("|!|", "\r\n"))

    val tempFileName = arg.dropRight(4) + ".csv"
    val tempFilePath = new org.apache.hadoop.fs.Path(workingDir.toString() + "/" + tempFileName)
    val exists = hdfs.exists(tempFilePath)
    if (exists) {
      hdfs.delete(tempFilePath, true)
    }

    newFileString.saveAsTextFile(new Path(workingDir ,tempFileName).toString())
    tempFileName
  }

   val result = sqlContext.sql(
    """
      select
          EANORGANIZATION.OrganizationID,
          EANINSTRUMENT.InstrumentID,
          EANQUOTE.QuoteID,
          EANQUOTE.TickerSymbol,
          EANQUOTE.MIC as IdentifierValue,
          EANORGANIZATION.MainQuoteId as OrgMainQuoteId,
          EANINSTRUMENT.MainQuoteId as InstrumentMainQuoteId,
          EANQUOTE.AssetState
          from EANORGANIZATION
          join EANINSTRUMENT on EANORGANIZATION.PreferredID=EANINSTRUMENT.PrefIssuedByOrganizationID
          and EANINSTRUMENT.RCSAssetClass='ORD'
          and UPPER(EANORGANIZATION.AdminStatus)='PUBLISHED'
          and UPPER(EANORGANIZATION.OrganizationStatusCode)='ACT'
          join EANQUOTE on EANQUOTE.PrefIsQuoteOfInstrumentID=EANINSTRUMENT.PreferredID
    """)

   val resultFMM = sqlContext.sql(
     """
       select
           EANFMMIDENTIFIER.IdentifierValue,
           EANFMMSOURCENAME.SourceName
           from EANFMMIDENTIFIER
           join EANFMMSOURCENAME on EANFMMSOURCENAME.SourceID=EANFMMIDENTIFIER.IdentifierEntityID
           and EANFMMIDENTIFIER.IdentifierTypeID=320556
           and UPPER(EANFMMSOURCENAME.NameType)='SHORT NAME'
           and EANFMMIDENTIFIER.EffectiveFrom<=current_timestamp()
           and EANFMMIDENTIFIER.EffectiveTo>current_timestamp()
     """)

   val tickerData = result.join(resultFMM, Seq("IdentifierValue"), "left_outer")
     .select(result("OrganizationID").as("OrganizationID"),
             result("InstrumentID").as("InstrumentID"),
             result("QuoteID").as("QuoteID"),
             result("TickerSymbol").as("TickerSymbol"),
             result("IdentifierValue").as("MIC"),
             result("OrgMainQuoteId").as("OrgMainQuoteId"),
             result("InstrumentMainQuoteId").as("InstrumentMainQuoteId"),
             result("AssetState").as("AssetState"),
             resultFMM("SourceName").as("SourceName"))

  // now simply write to a parquet file
  tickerData.write.mode(SaveMode.Overwrite).parquet(outputFilename)

  log.info("Wrote {}", outputFilename)
  outputFilename
}
