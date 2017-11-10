package com.thomsonreuters.ecp.cdap.workflows

import co.cask.cdap.api.ProgramStatus
import co.cask.cdap.api.workflow.{AbstractWorkflow, WorkflowContext}
import com.thomsonreuters.ecp.cdap.actions.HiveUpdaterScalaCustomAction

/**
  * Hive Updater Workflow.
  * Step 1. Read the message from the Kafka Topic (Scala Custom Action)
  * Step 2. Convert the Parquet file into a Hive table (Spark Program)
  */
class HiveUpdaterWorkflow extends AbstractWorkflow {

    final val CONSUMER_DESC: String = "Action to get ready message from the Kafka Topic so that the Hive table can be updated"

    @throws[Exception]
    override def initialize(context: WorkflowContext): Unit = { // Invoked before the Workflow run starts
      super.initialize(context)
    }

    override def configure(): Unit = {
      setName("HiveUpdaterWorkflow")
      setDescription("Hive Updater Workflow")
      addAction(new HiveUpdaterScalaCustomAction("ECPTickerFeedConsumer", CONSUMER_DESC))
      addSpark("ParquetToHive")
    }

    // Invoked after the execution of the Workflow
    override def destroy(): Unit = {
      val isWorkflowSuccessful = getContext.getState.getStatus eq ProgramStatus.COMPLETED
      val nodeStates = getContext.getNodeStates
    }

}
