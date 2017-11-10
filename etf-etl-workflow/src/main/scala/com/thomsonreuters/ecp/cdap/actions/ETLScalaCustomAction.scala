package com.thomsonreuters.ecp.cdap.actions

import co.cask.cdap.api.customaction.AbstractCustomAction
import com.thomsonreuters.ecp.cdap.actions.GetDataFromEAN._
import com.thomsonreuters.ecp.cdap.actions.ECPTickerFeedProducer._

/**
  * Wrapper for the Scala Custom Actions within the ETL-Workflow.
  *
  * @param name
  */
class ETLScalaCustomAction(name: String, description: String) extends AbstractCustomAction{

  override def run(): Unit = {
    if (name.equals("GetDataFromEAN")) {
      val fileList = getEANData()
      for (file <- fileList)
      {
        //getContext.getWorkflowToken.put("fileName",file)
        getContext.getRuntimeArguments.put("fileName",file)
      }
    } else if (name.equals("ECPTickerFeedProducer")) {
      //TODO:  need to update with all files
      val files = getContext.getWorkflowToken.getAll("fileName").get(1)
      sendFileMessageToEcpControl(files.toString)
    }
  }

  override def configure(): Unit = {
    setName(name)
    setDescription(description)
  }

}
