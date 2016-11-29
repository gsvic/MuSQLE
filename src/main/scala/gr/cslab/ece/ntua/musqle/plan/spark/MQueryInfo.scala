package gr.cslab.ece.ntua.musqle.plan.spark

import java.util.TreeMap

import gr.cslab.ece.ntua.musqle.engine.Engine
import gr.cslab.ece.ntua.musqle.plan.hypergraph.QueryInfo
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation

import scala.collection.mutable

final class MQueryInfo extends QueryInfo{
  var rootLogicalPlan: LogicalPlan = null
  val planToTableName : mutable.HashMap[LogicalRelation, String]  = new mutable.HashMap[LogicalRelation, String]()
  val tableNameToEngine : mutable.HashMap[String, Engine]  = new mutable.HashMap[String, Engine]()
  val tableMap: mutable.HashMap[Int, SparkPlanVertex] = new mutable.HashMap[Int, SparkPlanVertex]
  val attributeToVertex = new mutable.HashMap[String, SparkPlanVertex]
  val attributeToRelName = new mutable.HashMap[String, String]
  val idToCondition = new mutable.HashMap[Int, Expression]
  val idToVertex = new TreeMap[Int, SparkPlanVertex]()
  var lastCondition: Int = 1

  def reset() = {
    this.attributeToVertex.clear()
    this.idToCondition.clear()
    this.attributeToVertex.clear()
    this.attributeToRelName.clear()
    this.idToVertex.clear()
    this.tableMap.clear()
    this.lastCondition = 1
  }
}
