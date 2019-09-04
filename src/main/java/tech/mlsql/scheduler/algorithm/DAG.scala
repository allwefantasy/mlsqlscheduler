package tech.mlsql.scheduler.algorithm

import scala.collection.mutable.ArrayBuffer

/**
  * ref: https://www.cs.usfca.edu/~galles/visualization/TopoSortIndegree.html
  */
object DAG {
  def build[T <% Ordered[T]](items: Seq[(T, Option[T])]) = {

    val nodes = items.flatMap(item => Seq(item._1) ++ (item._2 match {
      case Some(value) => Seq(value)
      case None => Seq()
    })).distinct.sorted.map(id => JobNodePointer(JobNode(id, 0), None))
    val idToNode = nodes.map(f => (f.node.id, f)).toMap

    items.groupBy(f => f._1).map { idGroup =>
      val dependencies = idGroup._2.filter(_._2.isDefined).map(f => f._2.get)
      val target = idToNode(idGroup._1)
      dependencies.headOption match {
        case Some(value) =>
          val firstDep = JobNodePointer(idToNode(value).node, None)
          target.next = Option(firstDep)
          dependencies.drop(1).foldLeft(firstDep) { (a, b) =>
            val bPoiner = JobNodePointer(idToNode(b).node, None)
            a.next = Option(bPoiner)
            bPoiner
          }

        case None => target.next = None
      }
      target
    }
    new DAG[T](nodes)
  }
}

case class DAG[T <% Ordered[T]](nodes: Seq[JobNodePointer[T]]) {
  private def computeInDegree = {
    nodes.foreach { item =>
      var current = item.next
      while (current.isDefined) {
        current.get.node.inDegree += 1
        current = current.get.next
      }
    }
  }

  def dependencies[T](jobNodePointer: JobNodePointer[T]) = {
    var current = jobNodePointer
    val box = ArrayBuffer[JobNode[T]]()
    while (current.next.isDefined) {
      current = current.next.get
      box += current.node
    }
    box
  }

  def topologicalSort() = {
    val executeSequence = ArrayBuffer[JobNode[T]]()
    val zeroInDegreeStack = scala.collection.mutable.Stack[JobNode[T]]()
    computeInDegree
    nodes.filter(f => f.node.inDegree == 0).foreach { item => zeroInDegreeStack.push(item.node) }

    while (!zeroInDegreeStack.isEmpty) {
      val item = zeroInDegreeStack.pop()
      executeSequence += item
      var current = nodes.filter(_.node == item).head.next
      while (current.isDefined) {
        current.get.node.inDegree -= 1
        if (current.get.node.inDegree == 0) {
          zeroInDegreeStack.push(current.get.node)
        }
        current = current.get.next
      }

    }
    executeSequence
  }

}

case class JobNodePointer[T](node: JobNode[T], var next: Option[JobNodePointer[T]])

case class JobNode[T](val id: T, var inDegree: Int)
