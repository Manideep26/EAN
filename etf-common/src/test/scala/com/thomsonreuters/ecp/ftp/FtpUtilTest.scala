package com.thomsonreuters.ecp.ftp

import com.thomsonreuters.ecp.AppHelper
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class FtpUtilTest extends FlatSpec with Matchers with BeforeAndAfter {

  info("Starting FtpUtilTest...")

  val conf = AppHelper.makeAppConfig()

  val ftp = new FtpUtil()

  after {
    ftp.disconnect()
  }

  // FIXME: CFATS-636 This can be useful in a live environment, but we should also see about isolated tests. -Ed
  "An FtpUtil" should "connect" in {
    assert(ftp.canConnect(conf.getString("server")))
  }

  it should "connect with credentials and get a list of files" in {
    ftp.connectWithAuth(conf.getString("server"), conf.getString("username"), conf.getString("password"))
    ftp.cd(conf.getString("workingDirectory"))
    val fileList = ftp.listFiles()
    val fileListSize = fileList.size
    info(s"File List has $fileListSize items")
    assert(fileListSize > 0)
  }

  ignore should "be able to download a file" in {
    ftp.connectWithAuth(conf.getString("server"), conf.getString("username"), conf.getString("password"))
    ftp.cd(conf.getString("workingDirectory"))
    ftp.downloadFile("EANGEOGRAPHYNAME_20150305_1848.zip", "/ct-data/oa/oap/workspace/")
  }

}
