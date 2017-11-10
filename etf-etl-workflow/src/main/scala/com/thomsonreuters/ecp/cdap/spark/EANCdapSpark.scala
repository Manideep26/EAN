package com.thomsonreuters.ecp.cdap.spark

/**
  * Created by u0172104 on 10/10/2017.
  * CDAP Wrapper for ECP Ticker Feed ETL Workflow
  */
import co.cask.cdap.api.spark.AbstractSpark

import scala.collection.JavaConverters._

class EANCdapSpark(name: String, description: String, prefs: Map[String, String]) extends AbstractSpark {
  override def configure(): Unit = {
    setName(name)
    setDescription(description)
    setProperties(prefs.asJava)
    setMainClassName("com.thomsonreuters.ecp.cdap.spark.EANCdapSparkRunnerProxy")
  }
}

object EANCdapSpark {
  val DEFAULT_MEMORY = 4096 //MB
  val DEFAULT_CORES = 16
}

