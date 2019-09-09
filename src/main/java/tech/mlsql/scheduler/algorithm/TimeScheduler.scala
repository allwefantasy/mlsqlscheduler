package tech.mlsql.scheduler.algorithm

import java.util.TimeZone

import it.sauronsoftware.cron4j.{Scheduler, TaskCollector}

/**
  * 2019-09-05 WilliamZhu(allwefantasy@gmail.com)
  */
object TimeScheduler {
  val scheduler = new Scheduler()

  def start(tc: TaskCollector, timeZone: String) = {
    if (!scheduler.isStarted) {
      scheduler.setTimeZone(TimeZone.getTimeZone(timeZone))
      scheduler.addTaskCollector(tc)
      scheduler.start()
    }
  }

  def stop() = {
    scheduler.stop()
  }
}


