package gr.cslab.ece.ntua.musqle.plan.spark

import gr.cslab.ece.ntua.musqle.plan.hypergraph.DPJoinPlan
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.plans.logical.{Limit, LogicalPlan, Sort}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Created by vic on 17/11/2016.
  */
class Execution(sparkSession: SparkSession) {
  def execute(plan: DPJoinPlan): DataFrame = {
    executeMovements(plan)

    val root = plan.info.asInstanceOf[MQueryInfo].rootLogicalPlan
    val df = plan.engine.getDF(plan.toSQL)
    val musqlePlan = df.queryExecution.optimizedPlan

    val fixed = prepare(root, musqlePlan)

    new Dataset[Row](sparkSession, fixed, RowEncoder(df.queryExecution.analyzed.schema))
  }

  def prepare(plan: LogicalPlan, musqlePlan: LogicalPlan): LogicalPlan = {
    var current = musqlePlan
    var node = plan

    while (node.children.size < 2) {
      node match {
        case Limit(expression, child) => {
          current =  Limit(expression, current)
        }
        case _ => {}
      }
      node = node.children(0)
    }

    current
  }

  def executeMovements(plan: DPJoinPlan): Unit = {
    if (plan.left != null) executeMovements(plan.left)
    if (plan.right != null) executeMovements(plan.right)

    if (plan.isInstanceOf[MuSQLEMove]) {
      plan.engine.move(plan)
    }
  }
}
