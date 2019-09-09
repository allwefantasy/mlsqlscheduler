package tech.mlsql.scheduler.algorithm

import tech.mlsql.scheduler.{JobNode, JobNodePointer}

import scala.collection.mutable.ArrayBuffer

/**
  * ref: https://www.cs.usfca.edu/~galles/visualization/TopoSortIndegree.html
  * not thread safe
  */
object DAG {
  def build[T <% Ordered[T]](items: Seq[(T, Option[T])]) = {

    val nodes = items.flatMap(item => Seq(item._1) ++ (item._2 match {
      case Some(value) => Seq(value)
      case None => Seq()
    })).distinct.sorted.map(id => JobNodePointer(JobNode(id, 0, 0, Seq(false), Seq(false), Seq(""), None, ""), None))
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
    val instance = new DAG[T](nodes)
    instance.computeInDegree
    instance
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

  def outDegreeNodes[T](jobNodePointer: JobNodePointer[T]) = {
    var current = jobNodePointer
    val box = ArrayBuffer[JobNodePointer[T]]()
    while (current.next.isDefined) {
      current = current.next.get
      box += current
    }
    box
  }

  def findLeafNodeInTheSameTree(id: T) = {

    val sameTreeGroup = getRootNodes.map { pointer =>
      val inTheSameTree = scala.collection.mutable.HashSet[JobNode[T]]()
      travel(pointer.node.id, p => {
        if (p.next.isEmpty) {
          inTheSameTree += p.node
        }
      })
      inTheSameTree
    }

    val leafNodeInTheSameTree = sameTreeGroup.
      filter(inTheSameTree => inTheSameTree.contains(getNodePointer(id).node)).
      flatMap(f => f.toSeq).toSet

    leafNodeInTheSameTree
  }

  def findNodeInTheSameTree(id: T) = {
    val leafNodes = findLeafNodeInTheSameTree(id).map(f => f.id)
    val stack = new scala.collection.mutable.Stack[JobNode[T]]()
    getRootNodes.flatMap { pointer =>
      val inTheSameTree = scala.collection.mutable.HashSet[JobNode[T]]()
      travel(pointer.node.id, p => {
        stack.push(p.node)
        if (leafNodes.contains(p.node.id)) {
          stack.foreach(f => inTheSameTree += f)
          stack.clear()
        }
      })
      inTheSameTree
    }.toSet
  }

  def travel(id: T, visit: (JobNodePointer[T]) => Unit): Unit = {
    val rootPointer = getNodePointer(id)
    visit(rootPointer)
    val outs = outDegreeNodes(rootPointer)
    outs.foreach(f => travel(f.node.id, visit))
  }

  def getNodePointer(id: T) = {
    nodes.filter(_.node.id == id).head
  }

  def isCircleDependency(id: T, dependedNodeId: T): Boolean = {
    var exists = false
    travel(dependedNodeId, (pointer) => {
      if (pointer.node.id == id) {
        exists = true
      }
    })
    return exists

  }

  def isDependencyExists(id: T, dependedNodeId: T): Boolean = {
    isCircleDependency(dependedNodeId, id)
  }

  /**
    * get all nodes have no dependencies on them, so you can check
    * whether they all have been configured time-scheduler
    */
  def getRootNodes = {
    nodes.filter(f => f.node.inDegree == 0)
  }

  def getLeafNodes = {
    nodes.filterNot(f => f.next.isDefined)
  }

  def topologicalSort() = {
    val executeSequence = ArrayBuffer[JobNode[T]]()
    val zeroInDegreeStack = scala.collection.mutable.Stack[JobNode[T]]()

    //    getLeafNodes.foreach(pointer => {
    //      require(pointer.node.cron.isDefined, s"job ${pointer.node.id} should be configured time scheduler")
    //    })

    getRootNodes.foreach { item => zeroInDegreeStack.push(item.node) }

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


