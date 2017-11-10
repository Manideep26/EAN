package com.thomsonreuters.ecp.ftp
import java.io.InputStream

import com.thomsonreuters.ecp.AppHelper
import org.apache.commons.net.ftp._
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.util.Try


final class FtpUtil() {


  private val client = new FTPClient

  def fs : FileSystem = {
    if (_fs.isEmpty) {
      _fs = Some(AppHelper.makeHdfsFileSystem())
    }
    _fs.get
  }
  var _fs : Option[FileSystem] = Option.empty

  def login(username: String, password: String): Try[Boolean] = Try {
    client.login(username, password)
    client.setFileType(FTP.BINARY_FILE_TYPE)
    client.setFileTransferMode(FTP.BLOCK_TRANSFER_MODE)
  }

  def connect(host: String): Try[Unit] = Try {
    client.connect(host)
    client.enterLocalPassiveMode()
  }

  def connected: Boolean = client.isConnected

  def disconnect(): Unit = client.disconnect()

  def canConnect(host: String): Boolean = {
    client.connect(host)
    val connectionWasEstablished = connected
    client.disconnect()
    connectionWasEstablished
  }

  def listFiles(dir: Option[String] = None): List[FTPFile] =
    dir.fold(client.listFiles)(client.listFiles).toList

  def connectWithAuth(host: String,
                      username: String = "anonymous",
                      password: String = "") : Try[Boolean] = {
    for {
      connection <- connect(host)
      login      <- login(username, password)
    } yield login
  }

  def cd(path: String): Boolean =
    client.changeWorkingDirectory(path)

  def filesInCurrentDirectory: Seq[String] =
    listFiles().map(_.getName)

  def downloadFileStream(remote: String): InputStream = {
    val stream = client.retrieveFileStream(remote)
    client.completePendingCommand()
    stream
  }

  def downloadFile(remote: String, localPath: String): Boolean = {
    val os = fs.create(new Path(localPath, remote), true)
    val retVal = client.retrieveFile(remote, os)
    os.close()
    retVal
  }

}
