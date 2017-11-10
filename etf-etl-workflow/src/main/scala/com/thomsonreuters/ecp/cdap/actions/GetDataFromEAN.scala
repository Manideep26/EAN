package com.thomsonreuters.ecp.cdap.actions

import java.text.SimpleDateFormat
import java.util.GregorianCalendar
import java.util.zip.ZipInputStream

import com.thomsonreuters.ecp.AppHelper
import com.thomsonreuters.ecp.ftp.FtpUtil
import org.apache.commons.net.ftp.FTPFile
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

import collection.JavaConverters._

object GetDataFromEAN extends App{
  val log = LoggerFactory.getLogger(GetDataFromEAN.getClass)
  val ftp = new FtpUtil
  val conf = AppHelper.makeAppConfig()
  val fs = AppHelper.makeHdfsFileSystem()
  var fileList:List[String] = null
  getEANData()

  def getEANData(): List[String] = {
    val tables = conf.getStringList("eanTables").asScala.union(args.toList)
    if (tables.isEmpty) {
      log.warn("Please provide a list of files to get, ie. EANCURRENCYNAME, EANGEOGRAPHYNAME, as eanTables")
      sys.exit()
    }

    val workingDir = new Path(conf.getString("workingDirectory"))
    val fs = AppHelper.makeHdfsFileSystem()
    if (!fs.exists(workingDir)) {
      fs.mkdirs(workingDir)
    }

    tables.foreach(getFile(_, workingDir))

    // Close the FTP Connection
    ftp.disconnect()
    fileList
  }

  def getFile(filename: String, workingDir: Path): Unit = {
    log.info(s"Looking for file starting with $filename")
    val fileOption = getTodayFile(filename)
    fileOption.foreach(file => log.info(s"Today's file is $file"))
    connectFtpAndCd()
    fileOption.foreach(file => ftp.downloadFile(file.getName(), workingDir.toString))
    ftp.disconnect()

    // Unzip it
    fileOption.foreach(file => unzipFile(file.getName(), workingDir))
  }

  def getTodayFile(filename: String): Option[FTPFile] = {
    // Get the date format

    /*
     * TODO: CFATS-634 Use today's date rather than this manufactured one when we actually get new files
     * Actually, use the most recent date per file.
     */
    // val now = Calendar.getInstance()
    // Calendars take a 0-based month but 1-based days because WHY NOT.
    val now = new GregorianCalendar(2017, 9, 4, 0, 0, 0)

    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val todayFormatted = dateFormat.format(now.getTime())

    log.info(s"Using this formatted date: $todayFormatted")

    // Get a list of files
    connectFtpAndCd()

    val fileList = ftp.listFiles()
    ftp.disconnect()

    // Get a list of files matching today's date
    val todayFiles = fileList.filter(file => file.getName().startsWith(s"${filename}_${todayFormatted}"))
    if (todayFiles.isEmpty) {
      val allFiles = fileList.filter(file => file.getName().startsWith(s"${filename}_"))
      log.warn(s"Unable to find ANY matching files with the date ${todayFormatted}.  ALL matching files found: ${allFiles}")
      return None
    }
    log.info(s"Today $filename files list: $todayFiles")

    // Check if we have the ready file
    if (todayFiles.exists(file => file.getName().endsWith("txt"))) {
      return todayFiles.find(file => file.getName.endsWith("zip"))
    }

    return None
  }

  def connectFtpAndCd(): Unit = {
    ftp.connectWithAuth(conf.getString("server"), conf.getString("username"), conf.getString("password"))
    ftp.cd(conf.getString("ftpHomeDirectory"))
  }

  def unzipFile(filename: String, workingDir: Path): Unit = {
    val zipFile = fs.open(new Path(workingDir, filename))
    val zipInputStream = new ZipInputStream(zipFile)

    val zipNames = Stream.continually(zipInputStream.getNextEntry).takeWhile(_ != null).map{ file =>
      val zipName = file.getName
      log.info(s"Unzipping: $zipName")
      val hdfsPath = new Path(workingDir, zipName)
      val fileOut = fs.create(hdfsPath, true)
      val buffer = new Array[Byte](1024)
      Stream.continually(zipInputStream.read(buffer)).takeWhile(_ != -1).foreach(fileOut.write(buffer, 0, _))
      fileOut.close()
      log.info(s"Wrote $hdfsPath")

      zipName
    }
    zipInputStream.close()
    zipFile.close()

    log.info(s"Wrote to $workingDir: $zipNames")
    fileList = zipNames.toList
  }

}