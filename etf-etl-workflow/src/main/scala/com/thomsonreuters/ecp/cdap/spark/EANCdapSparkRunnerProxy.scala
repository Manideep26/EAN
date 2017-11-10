package com.thomsonreuters.ecp.cdap.spark

/**
  * Created by u0172104 on 10/10/2017.
  */
import co.cask.cdap.api.spark.{SparkExecutionContext, SparkMain}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
  * CDAP SparkMain that can pass runtime arguments to an existing spark program
  */
abstract class EANCdapSparkRunnerProxy extends SparkMain {
  val runnerClassName: String = "BCPToParquet"
  val logger = LoggerFactory.getLogger(getClass)

  def beforeLaunch()(implicit sec: SparkExecutionContext): Unit = {
    //Default nothing
  }

  override def run(implicit sec: SparkExecutionContext): Unit = {
    beforeLaunch()
    launchSpark(runnerClassName)
  }

  def launchSpark(runnerClassName: String)(implicit sec: SparkExecutionContext): Unit = {
    val (keys,args) = sec.getRuntimeArguments.asScala.toArray.unzip
    
    val runnerClass = Class.forName(runnerClassName)
    val mainMethod = runnerClass.getMethod("main", classOf[Array[String]])
    mainMethod.invoke(args)
  }
}
