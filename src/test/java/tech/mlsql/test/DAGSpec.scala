package tech.mlsql.test

import org.scalatest.FunSuite
import tech.mlsql.scheduler.algorithm.{DAG, JobNodePointer}

/**
  * 2019-09-04 WilliamZhu(allwefantasy@gmail.com)
  */
class DAGSpec extends FunSuite {
  //ref: https://www.cs.usfca.edu/~galles/visualization/TopoSortIndegree.html
  test("test dag alg") {

    val dagInstance = DAG.build[Int](Seq((0, Option(2)), (1, Option(2)), (1, Option(3)), (1, Option(6)), (2, Option(4)), (4, Option(6))))

    def getList(jobNodePointer: JobNodePointer[Int]) = {
      dagInstance.dependencies[Int](jobNodePointer).map(_.id)
    }

    assert(getList(dagInstance.nodes(0)).mkString(",") == "2")
    assert(getList(dagInstance.nodes(1)).mkString(",") == "2,3,6")
    assert(getList(dagInstance.nodes(2)).mkString(",") == "4")
    assert(getList(dagInstance.nodes(3)).mkString(",") == "")
    assert(getList(dagInstance.nodes(4)).mkString(",") == "6")
    assert(getList(dagInstance.nodes(5)).mkString(",") == "")
    val items = dagInstance.topologicalSort()
    assert(items(0).id == 1)
    assert(items(1).id == 3)
    assert(items(2).id == 0)
    assert(items(3).id == 2)
    assert(items(4).id == 4)
    assert(items(5).id == 6)
  }
}
