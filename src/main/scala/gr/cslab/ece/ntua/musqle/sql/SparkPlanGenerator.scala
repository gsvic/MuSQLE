package gr.cslab.ece.ntua.musqle.sql

import javax.naming.OperationNotSupportedException

import gr.cslab.ece.ntua.musqle.engine.Engine
import gr.cslab.ece.ntua.musqle.plan.hypergraph.DPJoinPlan
import gr.cslab.ece.ntua.musqle.plan.spark.{MQueryInfo, MuSQLEJoin, MuSQLEMove, MuSQLEScan}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.expressions.{Attribute, EqualTo, Expression, SortOrder}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.SQLConf

/**
  * Transformation of [[DPJoinPlan]] into [[LogicalPlan]]
  * */
class SparkPlanGenerator(sparkSession: SparkSession) {


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
          case Project(projectList, child) => {
            val sparkLogical = toSparkLogicalPlan(plan, child)
            val newProjectList = projectList.map{projection =>
              val name = projection.name
              val out = sparkLogical.output
              val r = out.find(name == _.name)
              r.get
            }
            new Project(newProjectList, sparkLogical)
          }
          case Sort(order, global, child) => {
            new Sort(order, global, toSparkLogicalPlan(plan, child))
          }
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

        val exp = makeNewExpression(left, right, expression.get)
        new Join(left, right, Inner, Option(exp))
      }
      case musqleScan: MuSQLEScan => {
        musqleScan.vertex.plan
      }
      case move: MuSQLEMove => {
        val m = move.dpJoinPlan.engine.getDF(move.dpJoinPlan.toSQL).queryExecution.optimizedPlan.asInstanceOf[LogicalRelation]
        val outputAttributes = sparkSession.sql(move.dpJoinPlan.toSQL).queryExecution.optimizedPlan.output
        val lr = new LogicalRelation(m.relation, Option(outputAttributes))
        lr
      }
    }
  }

  private def makeNewExpression(left: LogicalPlan, right: LogicalPlan, expression: Expression): Expression = {
    expression match {
      case EqualTo(leftKey, rightKey) => {
        var leftAttribute = null.asInstanceOf[Attribute]
        var rightAttribute = null.asInstanceOf[Attribute]

        left.output.foreach { k =>
          if (k.name.equals(leftKey.asInstanceOf[Attribute].name))
            leftAttribute = k
        }

        right.output.foreach { k =>
          if (k.name.equals(rightKey.asInstanceOf[Attribute].name))
            rightAttribute = k
        }

        EqualTo(leftAttribute, rightAttribute)
      }
      case _ => throw new OperationNotSupportedException("MuSQLE currently supports only inner joins!")
    }
  }
}
