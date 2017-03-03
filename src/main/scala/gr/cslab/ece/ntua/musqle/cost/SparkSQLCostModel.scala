package gr.cslab.ece.ntua.musqle.cost

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

/**
  * Created by vic on 3/11/2016.
  */
trait SparkSQLCostModel {
  val numberOfWorkers: Int
  val coresPerWorker: Int
  val memoryPerWorker: Int
  val metrics: CostModelMetrics
  val cost: Double


}

case class CostModelMetrics(val partitions: Int, val sizeInBytes: BigInt)

case class Scan(val workers: Int, val cores: Int, memory: Int, val costMetrics: CostModelMetrics)
  extends SparkSQLCostModel{
  override final val numberOfWorkers: Int = workers
  override final val coresPerWorker: Int = cores
  override final val memoryPerWorker: Int = memory
  override final val metrics: CostModelMetrics = costMetrics

  val numberOfRounds = Math.ceil(metrics.partitions / coresPerWorker) + 1

  override final val cost = (metrics.sizeInBytes.toDouble / metrics.partitions) * numberOfRounds
}

object SparkSQLCostModel{
  def prepareMetrics(plan: LogicalPlan): CostModelMetrics = {
    plan match{
      case logicalRelation: LogicalRelation => {
        logicalRelation.relation match {
          case hdfs: HadoopFsRelation => {
            val partitions = 1// hdfs.location.allFiles().size
            val sizeInBytes = logicalRelation.statistics.sizeInBytes
            CostModelMetrics(partitions, sizeInBytes)
          }
          case _ => CostModelMetrics(1, 1)
          case _ => {
            throw new Exception(s"matching error: ${logicalRelation.relation}")
          }
        }
      }
    }
  }
}