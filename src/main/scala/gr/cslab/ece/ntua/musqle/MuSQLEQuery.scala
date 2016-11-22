package gr.cslab.ece.ntua.musqle

import gr.cslab.ece.ntua.musqle.plan.hypergraph.DPJoinPlan
import gr.cslab.ece.ntua.musqle.plan.spark.Execution
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by vic on 22/11/2016.
  */
class MuSQLEQuery(val sparkSession: SparkSession, optimizedPlan: DPJoinPlan) {
  val sqlString: String = {
    optimizedPlan.toSQL
  }
  def execute: DataFrame = {
    val executor = new Execution(sparkSession)
    val result = executor.execute(optimizedPlan)

    result
  }
  def explain: Unit = {
    optimizedPlan.explain()
  }
}
