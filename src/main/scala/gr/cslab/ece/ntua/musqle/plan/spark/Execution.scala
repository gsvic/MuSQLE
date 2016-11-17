package gr.cslab.ece.ntua.musqle.plan.spark

import gr.cslab.ece.ntua.musqle.plan.hypergraph.DPJoinPlan
import org.apache.spark.sql.SparkSession

/**
  * Created by vic on 17/11/2016.
  */
class Execution(sparkSession: SparkSession) {
  def execute(plan: DPJoinPlan): Unit = {
    executeMovements(plan)
    println(s"Executing: ${plan.toSQL}")
  }
  def executeMovements(plan: DPJoinPlan): Unit = {
    if (plan.left != null) executeMovements(plan.left)
    if (plan.right != null) executeMovements(plan.right)

    if (plan.isInstanceOf[MuSQLEMove]) {
      plan.engine.move(plan)
    }
  }
}
