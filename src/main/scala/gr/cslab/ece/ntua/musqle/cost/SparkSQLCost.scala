package gr.cslab.ece.ntua.musqle.cost

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

class SparkSQLCost {
  def estimateCost(dataFrame: DataFrame): Double = {
    estimateCost(dataFrame.queryExecution.optimizedPlan)
  }

  private def estimateCost(logicalPlan: LogicalPlan): Double = {
    var cost = 0.0
    logicalPlan.children.foreach(x => cost += estimateCost(x))

    logicalPlan match {
      case lr: LogicalRelation => cost += getLogicalRelationCost(lr)
      case _ => {}
    }
    cost
  }

  private def getLogicalRelationCost(logicalRelation: LogicalRelation): Double = {
    val metrics = SparkSQLCostModel.prepareMetrics(logicalRelation)
    val scanCost = new Scan(9, 8, 4096, metrics)

    scanCost.cost
  }
}

