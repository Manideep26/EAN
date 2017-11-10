package com.thomsonreuters.ecp

import java.io.{FileOutputStream, FileWriter}

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import scala.collection.JavaConverters._

object AppHelper {
  def makeAppConfig() : Config = {
    ConfigFactory.load()
  }

  def makeHadoopConfiguration() : Configuration = {

    val conf = makeAppConfig()

    val hadoopConf = new Configuration()
    hadoopConf.set("fs.default.name", conf.getString("fs.default.name"))
    hadoopConf.set("fs.defaultFS", conf.getString("fs.default.name"))

    hadoopConf
  }

  def makeHdfsFileSystem() : FileSystem = {
    FileSystem.get(makeHadoopConfiguration())
  }
}
