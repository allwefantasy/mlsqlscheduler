package tech.mlsql.scheduler

case class JobNodePointer[T](node: JobNode[T], var next: Option[JobNodePointer[T]])

case class CronOp(cron: String)

case class JobNode[T](val id: T,
                      var inDegree: Int,
                      var outDegree: Int,
                      var isExecuted: Seq[Boolean],
                      var isSuccess: Seq[Boolean],
                      var msg: Seq[String],
                      var cron: Option[CronOp],
                      var owner: String

                     ) {
  def isLastSuccess = {
    isSuccess.size > 0 && isSuccess.last
  }

  def cleanStatus = {
    isExecuted = Seq()
    isSuccess = Seq()
    msg = Seq()
  }
}

case class TimerJob[T](owner: String, val id: T, val cron: String)

case class DependencyJob[T](owner: String, val id: T, val dependency: T)

