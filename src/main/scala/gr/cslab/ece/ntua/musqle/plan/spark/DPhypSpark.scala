package gr.cslab.ece.ntua.musqle.spark

import java.util

import gr.cslab.ece.ntua.musqle.engine.{Engine, Spark}
import gr.cslab.ece.ntua.musqle.plan.hypergraph._
import gr.cslab.ece.ntua.musqle.plan.spark._
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, EqualTo, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

import scala.collection.JavaConversions._
import scala.collection.mutable

class DPhypSpark(sparkSession: SparkSession) extends
  DPhyp(moveClass = classOf[MuSQLEMove],scanClass = classOf[MuSQLEScan], joinClass = classOf[MuSQLEJoin]) {

  //override var dptable = new DPTable(Seq())
  override var queryInfo: QueryInfo = new MQueryInfo()
  var qInfo: MQueryInfo = queryInfo.asInstanceOf[MQueryInfo]

  override def generateGraph(): Unit = {
    this.vertices.clear()
    this.edgeGraph.clear()
    this.location.clear()
    this.numberOfVertices = 0
    this.dptable = new DPTable(Seq())
    Vertex.resetId
    DPJoinPlan.zeroResultNumber


    if (qInfo.rootLogicalPlan == null) {
      throw new LogicalPlanNotSetException
    }
    else {
      val attrSet = new mutable.HashSet[Attribute]()
      qInfo.rootLogicalPlan.output.foreach(attrSet.add)

      generateGraph(qInfo.rootLogicalPlan, attrSet)

      qInfo.idToVertex.values().foreach{ vertex =>
        addVertex(vertex, vertex.connections.toList)
      }
    }
  }

  def generateGraph(logical: LogicalPlan, projections: mutable.HashSet[Attribute]): Unit ={
    logical.children.foreach{ node =>
      if (!logical.isInstanceOf[Filter]) {
        generateGraph(node, projections.union(logical.output.toSet))
      }
    }

    val finalProjections = projections.intersect(logical.output.toSet)

    logical match {
      /* Setting up vertices */
      case logicalRelation: LogicalRelation => {
        addScan(logicalRelation, null, finalProjections)
      }
      case filter: Filter => {
        addScan(filter.child.asInstanceOf[LogicalRelation], filter, finalProjections)
      }
      case join: Join => {
        addJoin(join)
      }
      case _ => {}
    }
  }


  def setLogicalPlan(logicalPlan: LogicalPlan): Unit ={
    this.qInfo.rootLogicalPlan = logicalPlan
  }
  private def addScan(logicalRelation: LogicalRelation, filter: Filter,
                      projections: mutable.HashSet[Attribute]): Unit ={
    val vertex = new SparkPlanVertex(logicalRelation, Seq(getEngine(logicalRelation)), filter, projections)
    qInfo.tableMap.put(logicalRelation.hashCode(), vertex)
    qInfo.idToVertex.put(vertex.id, vertex)

    logicalRelation.output.foreach{ attribute =>
      qInfo.attributeToVertex.put(attribute.toString(), vertex)
    }
  }

  private def addJoin(join: Join): Unit ={
    val left = join.left
    val right = join.right
    val joinType = join.joinType
    val condition = join.condition
    condition.get match{
      case and: And => {
        addJoin(new Join(left, right, joinType, Option(and.left)))
        addJoin(new Join(left, right, joinType, Option(and.right)))
      }
      case equalTo: EqualTo => {
        val leftAttribute: AttributeReference = extractAttributeReference(equalTo.left)
        val rightAttribute: AttributeReference = extractAttributeReference(equalTo.right)
        val leftVertex = qInfo.attributeToVertex.get(leftAttribute.toString()).get
        val rightVertex = qInfo.attributeToVertex.get(rightAttribute.toString()).get

        leftVertex.connections.add((qInfo.lastCondition, Seq(rightVertex.id)))
        rightVertex.connections.add((qInfo.lastCondition, Seq(leftVertex.id)))
        qInfo.idToCondition.put(qInfo.lastCondition, condition.get)
        qInfo.lastCondition += 1
      }
      case _ => throw new OperationNotSupportedException
    }
  }
  private def getEngine(logicalRelation: LogicalRelation): Engine = {
    if (logicalRelation.relation.toString.contains("JDBC")) { Engine.POSTGRES(sparkSession)}
    else { Engine.SPARK(sparkSession) }

  }

  private def extractAttributeReference(expr: Expression): AttributeReference ={
    if (!expr.isInstanceOf[AttributeReference])
      extractAttributeReference(expr.children(0))
    else
      expr.asInstanceOf[AttributeReference]
  }

  class LogicalPlanNotSetException extends Exception("Catalyst LogicalPlan is not set. Hint: Set a " +
    "LogicalPlan with DPhypSpark.setLogicalPlan method")

  class OperationNotSupportedException extends Exception("MuSQLE currently supports only equality joins!")
}