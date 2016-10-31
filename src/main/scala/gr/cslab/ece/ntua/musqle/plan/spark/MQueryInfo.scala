package gr.cslab.ece.ntua.musqle.plan.spark

import java.util.TreeMap

import gr.cslab.ece.ntua.musqle.plan.hypergraph.QueryInfo
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation

import scala.collection.mutable

final class MQueryInfo extends QueryInfo{
  var rootLogicalPlan: LogicalPlan = null
  val planToTableName : mutable.HashMap[LogicalRelation, String]  = new mutable.HashMap[LogicalRelation, String]()
  val tableMap: mutable.HashMap[Int, SparkPlanVertex] = new mutable.HashMap[Int, SparkPlanVertex]
  val attributeToVertex = new mutable.HashMap[String, SparkPlanVertex]
  val idToCondition = new mutable.HashMap[Int, Expression]
  val idToVertex = new TreeMap[Int, SparkPlanVertex]()
  var lastCondition: Int = 1
}
