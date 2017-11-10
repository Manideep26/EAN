package com.thomsonreuters.ecp.cdap.app

import co.cask.cdap.api.app.AbstractApplication
import co.cask.cdap.api.schedule.Schedules
import com.thomsonreuters.ecp.cdap.workflows.HiveUpdaterWorkflow

class ECPTickerFeedHiveUpdaterApp extends AbstractApplication {
  def configure() {
    addWorkflow(new HiveUpdaterWorkflow)
    scheduleWorkflow(Schedules.builder("FiveHourSchedule")
      .setDescription("Schedule running every 60 seconds")
      .createTimeSchedule("0 0 */60 * *"),
      "HiveUpdaterWorkflow");
    /*CDAP 4.3*/
    //schedule(
    // buildSchedule("FiveHourSchedule", ProgramType.WORKFLOW, "MyWorkflow")
    //  .setDescription("Schedule running every 5 hours")
    //  .triggerByTime("0 */5 * * *"));
  }
}
