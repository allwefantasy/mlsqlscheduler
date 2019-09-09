package tech.mlsql.test

import org.scalatest.FunSuite
import tech.mlsql.scheduler.algorithm.DAG
import tech.mlsql.scheduler.{JobNode, JobNodePointer}

import scala.collection.mutable.ArrayBuffer

/**
  * 2019-09-04 WilliamZhu(allwefantasy@gmail.com)
  */
class DAGSpec extends FunSuite {
  //ref: https://www.cs.usfca.edu/~galles/visualization/TopoSortIndegree.html
  test("test dag alg") {

    val dagInstance = DAG.build[Int](Seq(
      (0, Option(2)),
      (1, Option(2)),
      (1, Option(3)),
      (1, Option(6)),
      (2, Option(4)),
      (4, Option(6)),
      (7, Option(8))
    ))

    def getList(jobNodePointer: JobNodePointer[Int]) = {
      dagInstance.outDegreeNodes[Int](jobNodePointer).map(_.node.id)
    }

    assert(getList(dagInstance.nodes(0)).mkString(",") == "2")
    assert(getList(dagInstance.nodes(1)).mkString(",") == "2,3,6")
    assert(getList(dagInstance.nodes(2)).mkString(",") == "4")
    assert(getList(dagInstance.nodes(3)).mkString(",") == "")
    assert(getList(dagInstance.nodes(4)).mkString(",") == "6")
    assert(getList(dagInstance.nodes(5)).mkString(",") == "")
    assert(getList(dagInstance.nodes(6)).mkString(",") == "8")

    //dagInstance.getLeafNodes.foreach(f => f.node.cron = Option(CronOp("")))

    val items = dagInstance.topologicalSort()
    assert(items(0).id == 7)
    assert(items(1).id == 8)
    assert(items(2).id == 1)
    assert(items(3).id == 3)
    assert(items(4).id == 0)
    assert(items(5).id == 2)
    assert(items(6).id == 4)
    assert(items(7).id == 6)

    assert(dagInstance.isCircleDependency(4, 0) == true)
    assert(dagInstance.isCircleDependency(3, 0) == false)
    assert(dagInstance.isDependencyExists(0, 4) == true)

    assert(dagInstance.findLeafNodeInTheSameTree(6).map(f => f.id).mkString(",") == "6,3")

    println(dagInstance.findNodeInTheSameTree(6).map(f => f.id).mkString(","))
  }

  //ref: https://www.cs.usfca.edu/~galles/visualization/TopoSortIndegree.html
  test("test 2 dag") {

    val dagInstance = DAG.build[Int](Seq(
      (0, Option(2)),
      (1, Option(0)),
      (1, Option(3)),
      (4, Option(5)),
      (6, Option(5))
    ))

    def getList(jobNodePointer: JobNodePointer[Int]) = {
      val arrayBuffer = ArrayBuffer[JobNode[Int]]()
      dagInstance.travel(jobNodePointer.node.id, p => arrayBuffer += p.node)
      arrayBuffer.drop(1).distinct.map(f => f.id)
    }

    assert(getList(dagInstance.nodes(0)).mkString(",") == "2")
    assert(getList(dagInstance.nodes(1)).mkString(",") == "0,2,3")
    assert(getList(dagInstance.nodes(2)).mkString(",") == "")
    assert(getList(dagInstance.nodes(3)).mkString(",") == "")
    assert(getList(dagInstance.nodes(4)).mkString(",") == "5")
    assert(getList(dagInstance.nodes(5)).mkString(",") == "")
    assert(getList(dagInstance.nodes(6)).mkString(",") == "5")

    //dagInstance.getLeafNodes.foreach(f => f.node.cron = Option(CronOp("")))

    val items = dagInstance.topologicalSort().reverse

    assert(dagInstance.findLeafNodeInTheSameTree(2).map(f => f.id).mkString(",") == "2,3")

    assert(dagInstance.findNodeInTheSameTree(2).map(f => f.id).mkString(",") == "0,2,3,1")
    val filter = dagInstance.findNodeInTheSameTree(2).map(f => f.id).toSet
    assert(items.filter(f => filter.contains(f.id)).map(f => f.id).mkString(",")=="2,0,3,1")

  }

}
