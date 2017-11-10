package com.thomsonreuters.ecp.cdap.workflows

import co.cask.cdap.api.ProgramStatus
import co.cask.cdap.api.workflow.{AbstractWorkflow, WorkflowContext}
import com.thomsonreuters.ecp.cdap.actions.ETLScalaCustomAction

/**
  * ECP Ticker Feed ETL Workflow.
  * Step 1. Get the data from EAN (Scala Custom Action)
  * Step 2. Convert the BCP files that we're collected from EAN into a single Parquet file (Spark Program)
  * Step 3.  Put a ready message on the Kafka topic to single the Parquet file has been updated. (Scala Custom Action)
  */
class ECPTickerFeedETLWorkflow extends AbstractWorkflow {

    final val GET_DATA_DESC: String = "Action to get the daily files from EAN FTP Site for processing into a Hive table"
    final val PRODUCER_DESC: String = "Action to send a ready message to the Kafka Topic that the Ticker Feed Parquet tables are ready.  Message will be picked up by the Hive Updater Workflow"

    @throws[Exception]
    override def initialize(context: WorkflowContext): Unit = { // Invoked before the Workflow run starts
      super.initialize(context)
    }

    override def configure(): Unit = {
      setName("EANETLWorkflow")
      setDescription("EAN to Neptune ETL Workflow")
      addAction(new ETLScalaCustomAction("GetDataFromEAN", GET_DATA_DESC))
      addSpark("BCPToParquet")
      addAction(new ETLScalaCustomAction("ECPTickerFeedProducer", PRODUCER_DESC))
    }

    // Invoked after the execution of the Workflow
    override def destroy(): Unit = {
      val isWorkflowSuccessful = getContext.getState.getStatus eq ProgramStatus.COMPLETED
      val nodeStates = getContext.getNodeStates
    }
}
