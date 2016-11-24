package gr.cslab.ece.ntua.musqle.plan.spark

import gr.cslab.ece.ntua.musqle.plan.hypergraph.DPJoinPlan
import gr.cslab.ece.ntua.musqle.sql.SQLCodeGen
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Limit, LogicalPlan, Sort}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Created by vic on 17/11/2016.
  */
class Execution(sparkSession: SparkSession) {
  val logger = Logger.getLogger(classOf[Execution])
  def execute(plan: DPJoinPlan): DataFrame = {

    executeMovements(plan)
    val root = plan.info.asInstanceOf[MQueryInfo].rootLogicalPlan
    val sql = plan.toSQL
    println(s"Executing: ${sql}")
    val df = plan.engine.getDF(sql)
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
