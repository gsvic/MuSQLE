package gr.cslab.ece.ntua.musqle.plan.hypergraph

import gr.cslab.ece.ntua.musqle.engine._
import gr.cslab.ece.ntua.musqle.plan.spark.MQueryInfo

import scala.collection.mutable

/**
  * An example DPhyp implementation
  */
class DPhypImpl(scan: Class[_], move: Class[_], join: Class[_]) extends DPhyp(){
  override var queryInfo: MQueryInfo = null

  val eng = Seq(Spark(null, null))
  val eng2 = Seq(Postgres(null, null))

  override def generateGraph(): Unit ={
    //addVertex(VertexImpl((eng, ""), List((1, Seq(2))))

    //addVertex(VertexImpl(eng, ""), List((1, Seq(1)),(2, Seq(3))))

    //addVertex(VertexImpl(eng2, ""), List((2, Seq(2))))

    /*
    addVertex(VertexImpl(eng2), List( (2, Seq(2,3)), (3, Seq(5))))

    addVertex(VertexImpl(eng2).setName("theTable"), List((3, Seq(3,4,7)), (4, Seq(6))))

    addVertex(VertexImpl(eng), List( (4, Seq(5)), (12, Seq(8)) ))
    addVertex(VertexImpl(eng), List( (4, Seq(6)), (3, Seq(5)) ))

    addVertex(VertexImpl(eng).setName("aSimpleNode"), List((12, Seq(6))))
    */

  }

}

object dphyp extends App {
  case class CustomScan(val vert: Vertex, val eng: Engine, val queryInfo: QueryInfo) extends Scan(vert, eng, queryInfo)
  case class CustomMove(vert: DPJoinPlan, override val engine: Engine, val queryInfo: QueryInfo)
    extends Move(vert, engine, queryInfo)
  case class CustomJoin(override val left: DPJoinPlan, override val right: DPJoinPlan,
                      override val vars: mutable.HashSet[Int], override val engine: Engine, val queryInfo: QueryInfo)
    extends Join(left,right, vars, engine, queryInfo)

  val opt = new DPhypImpl(classOf[CustomScan], classOf[CustomMove], classOf[CustomJoin])
  val p = opt.plan()
  p.explain()

}
