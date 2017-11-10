package com.thomsonreuters.ecp.cdap.actions

import co.cask.cdap.api.customaction.AbstractCustomAction
import com.thomsonreuters.ecp.cdap.actions.ECPTickerFeedConsumer._

/**
  * Wrapper for the Scala Custom Actions within the Hive Updater Workflow.
  *
  * @param name
  */
class HiveUpdaterScalaCustomAction(name: String, description: String) extends AbstractCustomAction{

  override def run(): Unit = {
    if (name.equals("ECPTickerFeedConsumer")) {
      //TODO:  need to determine what will accept the JSON output
      //readECPTickerFeedControlTopic()
    }
  }

  override def configure(): Unit = {
    setName(name)
    setDescription(description)
  }

}
