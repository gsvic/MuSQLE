package gr.cslab.ece.ntua.musqle

import gr.cslab.ece.ntua.musqle.plan.hypergraph.{DPJoinPlan, Join, Move}
import gr.cslab.ece.ntua.musqle.plan.spark._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

/**
  * Created by vic on 22/11/2016.
  */
class MuSQLEQuery(val sparkSession: SparkSession, optimizedPlan: DPJoinPlan) {
  val sqlString: String = {
    optimizedPlan.toSQL
  }
  def execute: DataFrame = {
    val executor = new Execution(sparkSession)
    pushDownProjections(optimizedPlan)
    val result = executor.execute(optimizedPlan)

    result
  }
  def explain: Unit = {
    optimizedPlan.explain()
  }

  private def pushDownProjections(plan: DPJoinPlan): Unit = {
    val root = plan.info.asInstanceOf[MQueryInfo].rootLogicalPlan
    var project = root
    while (!project.isInstanceOf[Project]) project = project.children(0)

    plan.projections.clear()
    project.asInstanceOf[Project].projectList.foreach(p =>
      if (plan.info.asInstanceOf[MQueryInfo].attributeToVertex.contains(p.toString())) {
        plan.projections.add(p.toString.replace("#", ""))
      }

    )
    pushDown(plan, plan.projections)
  }

  private def pushDown(plan: DPJoinPlan, projections: mutable.HashSet[String]): Unit = {
    plan.projections = plan.projections.intersect(projections)
    plan match {
      case join: MuSQLEJoin => {
        val cond = join.info.idToCondition(join.vars.toList(0))
        val attrs = extractAttributesFromCondition(cond)
        pushDown(join.left, plan.projections.union(attrs))
        pushDown(join.right, plan.projections.union(attrs))
      }
      case move: MuSQLEMove => {
        pushDown(move.left, plan.projections)
      }
      case scan: MuSQLEScan => {}
    }
  }

  private def extractAttributesFromCondition(expression: Expression): mutable.HashSet[String] = {
    val set = new mutable.HashSet[String]()

    expression.children.foreach{hs => extractAttributesFromCondition(hs).foreach(set.add)}
    expression match {
      case ar: AttributeReference => {
        set.add(ar.toString.replace("#", ""))
      }
      case _ => mutable.HashSet.empty
    }
    set
  }
}
