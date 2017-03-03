package gr.cslab.ece.ntua.musqle.cost

import gr.cslab.ece.ntua.musqle.MuSQLEContext
import gr.cslab.ece.ntua.musqle.tools.ConfParser
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, SortMergeJoinExec}

/***
  * Spark SQL Cost Models Library
  */

class SparkSQLCost(mc: MuSQLEContext) {
  def getCostMetrics(dataFrame: DataFrame): CostMetrics = {
    val cost = estimateCost(dataFrame.queryExecution.sparkPlan)
    cost
  }

  /**
    * Analyzes and estimates the cost for an input [[SparkPlan]]
    * */
  private def estimateCost(plan: SparkPlan): CostMetrics = {

    plan match {
      case scan: DataSourceScanExec => {
        val rows = {
          scan.relation match {
            case hdfs: HadoopFsRelation => {
              val path = hdfs.location.rootPaths(0).toUri.toString
              val table = mc.catalog.getTableEntryByPath(path)
              table.rows
            }
            case _ => {
              10000
            }
          }
        }

        val cm = CostMetrics(rows, 1)
        val cost = ScanCost(scan, cm)
        CostMetrics(rows, cost.getCost)
      }

      case filter: FilterExec => {
        val metrics = estimateCost(filter.child)
        val rows = Math.ceil(metrics.rows * (1/3.0)).toLong
        val filterMetrics = CostMetrics(rows, metrics.totalCost)

        filterMetrics
      }

      case join: SortMergeJoinExec => {
        val left = estimateCost(join.left)
        val right = estimateCost(join.right)

        val rows = Math.min(left.rows, right.rows)
        val cost = SortMergeJoinCost(left, right)

        CostMetrics(rows, cost.getCost)
      }

      case join: BroadcastHashJoinExec => {
        val left = estimateCost(join.left)
        val right = estimateCost(join.right)

        val rows = Math.min(left.rows, right.rows)
        val cost = BroadcastHashJoinCost(left, right)

        CostMetrics(rows, cost.getCost)
      }

      case join: BroadcastNestedLoopJoinExec =>  {
        val left = estimateCost(join.left)
        val right = estimateCost(join.right)

        val rows = Math.min(left.rows, right.rows)
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


  case class ScanCost(scan: DataSourceScanExec, metrics: CostMetrics) extends Cost{
    scan.relation match {
      case hadoopFsRelation: HadoopFsRelation => {
        val partitions = hadoopFsRelation.inputFiles.size
      }
      case _ => {}
    }
    override def getCost: Double = metrics.rows
  }

  case class SortMergeJoinCost(left: CostMetrics, right: CostMetrics) extends JoinCost{
    override def getCost: Double = {
      val sort = java.lang.Double.valueOf(ConfParser.getConf("spark.cost.sort").get)
      val merge = java.lang.Double.valueOf(ConfParser.getConf("spark.cost.merge").get)

      val leftSortCost = left.rows * Math.log(left.rows.toDouble) * sort
      val rightSortCost = right.rows * Math.log(right.rows.toDouble) * sort
      val mergeCost = Math.min(left.rows, right.rows) * merge

      val totalCost = leftSortCost + rightSortCost + mergeCost

      totalCost
    }
  }

  case class BroadcastHashJoinCost(left: CostMetrics, right: CostMetrics) extends JoinCost{
    override def getCost: Double = {
      val hashCostFactor = java.lang.Double.valueOf(ConfParser.getConf("spark.cost.hash").get)
      val mergeFactor = java.lang.Double.valueOf(ConfParser.getConf("spark.cost.merge").get)

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

  case class CostMetrics(val rows: Long, val totalCost: Double)
}

