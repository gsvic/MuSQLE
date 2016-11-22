package gr.cslab.ece.ntua.musqle.cost

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, SortMergeJoinExec}

class SparkSQLCost {
  def getCostMetrics(dataFrame: DataFrame): CostMetrics = {
    val cost = estimateCost(dataFrame.queryExecution.sparkPlan)
    cost
  }

  private def estimateCost(plan: SparkPlan): CostMetrics = {

    plan match {
      case scan: DataSourceScanExec => {
        val rows = scan.relation.sizeInBytes.toInt
        val cost = ScanCost(scan)
        CostMetrics(rows, cost.getCost)
      }
      case filter: FilterExec => {
        val metrics = estimateCost(filter.child)
        val filterMetrics = CostMetrics(metrics.rows * (1/3), metrics.totalCost)

        filterMetrics
      }
      case join: SortMergeJoinExec => {
        val left = estimateCost(join.left)
        val right = estimateCost(join.right)

        val rows = left.rows + right.rows
        val cost = SortMergeJoinCost(left, right)

        CostMetrics(rows, cost.getCost)
      }
      case join: BroadcastHashJoinExec => {
        val left = estimateCost(join.left)
        val right = estimateCost(join.right)

        val rows = left.rows + right.rows
        val cost = BroadcastHashJoinCost(left, right)

        CostMetrics(rows, cost.getCost)
      }
      case join: BroadcastNestedLoopJoinExec =>  {
        val left = estimateCost(join.left)
        val right = estimateCost(join.right)

        val rows = left.rows + right.rows
        val cost = BroadcastNestedLoopJoinCost(left, right)

        CostMetrics(rows, cost.getCost)
      }
      case _ => {
        estimateCost(plan.children(0))
      }
    }
  }


  trait Cost {
    def getCost: Double
  }
  trait JoinCost extends Cost {
    val left: CostMetrics
    val right: CostMetrics
  }


  case class ScanCost(scan: DataSourceScanExec) extends Cost{
    scan.relation match {
      case hadoopFsRelation: HadoopFsRelation => {
        val partitions = hadoopFsRelation.inputFiles.size
      }
      case _ => {}
    }
    override def getCost: Double = scan.relation.sizeInBytes
  }
  case class SortMergeJoinCost(left: CostMetrics, right: CostMetrics) extends JoinCost{
    override def getCost: Double = {
      val leftSortCost = left.rows * Math.log(left.rows.toDouble)
      val rightSortCost = right.rows * Math.log(right.rows.toDouble)
      val mergeCost = left.rows + right.rows

      val totalCost = leftSortCost + rightSortCost + mergeCost

      totalCost
    }
  }
  case class BroadcastHashJoinCost(left: CostMetrics, right: CostMetrics) extends JoinCost{
    override def getCost: Double = {
      val hashCostFactor = 0.001
      val mergeFactor = 0.001

      val hashCost = left.rows * hashCostFactor
      val mergeCost = right.rows * mergeFactor

      val totalCost = mergeCost + hashCost

      totalCost
    }
  }
  case class BroadcastNestedLoopJoinCost(left: CostMetrics, right: CostMetrics) extends JoinCost{
    override def getCost: Double = {
      left.totalCost + right.totalCost
    }
  }

  case class CostMetrics(val rows: Integer, val totalCost: Double)
}

