package gr.cslab.ece.ntua.musqle.plan.spark

import gr.cslab.ece.ntua.musqle.engine.Engine
import gr.cslab.ece.ntua.musqle.plan.hypergraph.{DPJoinPlan, Move}

/**
  * Created by vic on 19/10/2016.
  */
case class MuSQLEMove(val plan: DPJoinPlan, val destEngine: Engine, override val info: MQueryInfo)
  extends Move(plan, destEngine, info){
  override def toSQL: String = plan.toSQL

  this.projections = plan.projections

  val start = System.currentTimeMillis()
  destEngine.inject(this)
  val end = System.currentTimeMillis() - start
  MuSQLEMove.totalInject += end / 1000.0

  override def toString: String = {
    s"Move [${plan}](${plan.engine} -> ${this.engine})"
  }
}

object MuSQLEMove{
  var totalInject = 0.0
}