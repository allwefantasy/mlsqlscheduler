package tech.mlsql.scheduler.client

import tech.mlsql.scheduler.JobNode


/**
  * 2019-09-05 WilliamZhu(allwefantasy@gmail.com)
  */
trait ExecutorClient[T] {
  def execute(job: JobNode[T]): Unit
}
