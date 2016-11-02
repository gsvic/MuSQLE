package gr.cslab.ece.ntua.musqle.sql

import gr.cslab.ece.ntua.musqle.plan.hypergraph.DPJoinPlan
import gr.cslab.ece.ntua.musqle.plan.spark.{MQueryInfo, MuSQLEJoin, MuSQLEScan}
import org.apache.spark.sql.catalyst.plans.{Inner}
import org.apache.spark.sql.catalyst.plans.logical._

/**
  * Transformation of [[DPJoinPlan]] into [[LogicalPlan]]
  * */
class SparkPlanGenerator() {

  /** Generates a Catalyst LogicalPlan from a DPJoinPlan */
  def toSparkLogicalPlan(plan: DPJoinPlan): LogicalPlan = {
    val info = plan.info.asInstanceOf[MQueryInfo]
    val child = toSparkLogicalPlan(plan, info.rootLogicalPlan)
    child
  }

  /**
    * Generates a LogicalPlan from a DPJoinPlan as follows:
    * 1. Create the first part of the tree consisting of all the [[UnaryNode]]s, e.g. [[Project]]s, [[Sort]]s etc.
    * 2. The last [[UnaryNode]] visited takes as child the optimized [[DPJoinPlan]] as a Catalyst [[LogicalPlan]]
  * */
  private def toSparkLogicalPlan(plan: DPJoinPlan, logicalPlan: LogicalPlan): LogicalPlan = {
    logicalPlan match{
      case unaryNode: UnaryNode => {
        unaryNode match {
          case Project(projectList, child) => { new Project(projectList, toSparkLogicalPlan(plan, child)) }
          case Sort(order, global, child) => { new Sort(order, global, toSparkLogicalPlan(plan, child)) }
          case GlobalLimit(limit, child) => { new GlobalLimit(limit, toSparkLogicalPlan(plan, child)) }
          case LocalLimit(limit, child) => { new LocalLimit(limit, toSparkLogicalPlan(plan, child)) }
          case Aggregate(groupingExpressions, aggregateExpressions, child) => {
            new Aggregate(groupingExpressions, aggregateExpressions,toSparkLogicalPlan(plan, child)) }
        }
      }
      case _ => {
        dpJoinPlanToLogicalPlan(plan)
      }
    }
  }

  private def dpJoinPlanToLogicalPlan(plan: DPJoinPlan): LogicalPlan = {
    val qInfo = plan.info.asInstanceOf[MQueryInfo]
    plan match {
      case musqleJoin: MuSQLEJoin => {
        val left = dpJoinPlanToLogicalPlan(plan.left)
        val right = dpJoinPlanToLogicalPlan(plan.right)
        val expression = plan.info.asInstanceOf[MQueryInfo].idToCondition.get(musqleJoin.vars.toList(0))
        new Join(left, right, Inner, expression)
      }
      case musqleScan: MuSQLEScan => {
        musqleScan.vertex.plan
      }
    }
  }
}
