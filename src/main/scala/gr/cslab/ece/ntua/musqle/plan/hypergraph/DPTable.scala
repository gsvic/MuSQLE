package gr.cslab.ece.ntua.musqle.plan.hypergraph

import java.util
import gr.cslab.ece.ntua.musqle.engine.Engine
import scala.collection.JavaConversions._

protected class DPTable(val engines: Seq[Engine]) {
  private val dptable: util.HashMap[util.BitSet, util.HashMap[Engine, DPJoinPlan]] =
    new util.HashMap[util.BitSet, util.HashMap[Engine, DPJoinPlan]]


  def getOptimalPlan(engine: Engine, b: util.BitSet): DPJoinPlan = {
    val v: util.HashMap[Engine, DPJoinPlan] = dptable.get(b)
    if (v == null) return null
    v.get(engine)
  }

  def getOptimalPlan(b: util.BitSet): DPJoinPlan = {
    val v: util.HashMap[Engine, DPJoinPlan] = dptable.get(b)
    if (v == null) return null
    var ret: DPJoinPlan = null
    for (e <- v.entrySet) {
      if (ret == null) ret = e.getValue
      else if (ret.getCost > e.getValue.getCost) ret = e.getValue
    }
    ret
  }

  def getAllPlans(b: util.BitSet): util.HashMap[Engine, DPJoinPlan] = {
    val v: util.HashMap[Engine, DPJoinPlan] = dptable.get(b)
    if (v == null) return null
    v
  }

  def checkAndPut(engine: Engine, b: util.BitSet, plan: DPJoinPlan) {
    var v: util.HashMap[Engine, DPJoinPlan] = dptable.get(b)
    if (v == null) {
      v = new util.HashMap[Engine, DPJoinPlan]
      v.put(engine, plan)
      dptable.put(b, v)
      return
    }
    val v1: DPJoinPlan = v.get(engine)
    if (v1 == null) {
      v.put(engine, plan)
      dptable.put(b, v)
      return
    }
    if (v1.getCost > plan.getCost) v.put(engine, plan)
  }

  def print() {
    System.out.println(dptable)
  }
}
