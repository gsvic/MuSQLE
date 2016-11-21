package gr.cslab.ece.ntua.musqle.plan.spark

import gr.cslab.ece.ntua.musqle.engine.Engine
import gr.cslab.ece.ntua.musqle.plan.hypergraph.{DPJoinPlan, Move}

/**
  * Created by vic on 19/10/2016.
  */
case class MuSQLEMove(val plan: DPJoinPlan, val destEngine: Engine, override val info: MQueryInfo)
  extends Move(plan, destEngine, info){
  override val toSQL: String = plan.toSQL

  val start = System.currentTimeMillis()
  destEngine.inject(this)
  val end = System.currentTimeMillis() - start
  MuSQLEMove.totalInject += end / 1000.0
}

object MuSQLEMove{
  var totalInject = 0.0
}