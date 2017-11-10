package com.thomsonreuters.ecp.cdap.app

import co.cask.cdap.api.app.AbstractApplication
import co.cask.cdap.api.schedule.Schedules
import com.thomsonreuters.ecp.cdap.workflows.ECPTickerFeedETLWorkflow

class ECPTickerFeedETLApp extends AbstractApplication {
  def configure() {
    addWorkflow(new ECPTickerFeedETLWorkflow)
    scheduleWorkflow(Schedules.builder("FiveHourSchedule")
      .setDescription("Schedule running every 5 hours")
      .createTimeSchedule("0 */5 * * *"),
      "ECPTickerFeedETLWorkflow");
    /*CDAP 4.3*/
    //schedule(
    // buildSchedule("FiveHourSchedule", ProgramType.WORKFLOW, "MyWorkflow")
    //  .setDescription("Schedule running every 5 hours")
    //  .triggerByTime("0 */5 * * *"));
  }
}
