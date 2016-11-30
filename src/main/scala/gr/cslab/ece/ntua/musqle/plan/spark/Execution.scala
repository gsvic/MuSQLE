package gr.cslab.ece.ntua.musqle.plan.spark

import gr.cslab.ece.ntua.musqle.plan.hypergraph.DPJoinPlan
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.plans.logical.{Limit, LogicalPlan}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * The execution context
  * @param sparkSession: The current SparkSession
  */
class Execution(sparkSession: SparkSession) {
  val logger = Logger.getLogger(classOf[Execution])

  /** Executes a [[DPJoinPlan]] and returns the result as a Spark SQL [[DataFrame]] */
  def execute(plan: DPJoinPlan): DataFrame = {
    //Execute movements first, then execute the final query plan
    executeMovements(plan)
    val root = plan.info.asInstanceOf[MQueryInfo].rootLogicalPlan
    val sql = plan.toSQL

    logger.info(s"Executing: ${sql}")

    val df = plan.engine.getDF(sql)
    val musqlePlan = df.queryExecution.optimizedPlan

    val fixed = prepare(root, musqlePlan)

    new Dataset[Row](sparkSession, fixed, RowEncoder(df.queryExecution.analyzed.schema))
  }

  /** Prepares a plan - Adds a logical limit if needs */
  def prepare(plan: LogicalPlan, musqlePlan: LogicalPlan): LogicalPlan = {
    var current = musqlePlan
    var node = plan

    while (node.children.size < 2 && node.children.size > 0) {
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

  /** This method performs the required data movements
    * among the engines before the execution of the final query.*/
  def executeMovements(plan: DPJoinPlan): Unit = {
    logger.info("Movements...")
    if (plan.left != null) executeMovements(plan.left)
    if (plan.right != null) executeMovements(plan.right)

    if (plan.isInstanceOf[MuSQLEMove]) {
      plan.engine.move(plan)
    }
  }
}
