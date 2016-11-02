package gr.cslab.ece.ntua.musqle.plan.hypergraph

import gr.cslab.ece.ntua.musqle.engine.Engine
import scala.collection.mutable

abstract class DPJoinPlan(val left: DPJoinPlan, val right: DPJoinPlan, val engine: Engine,
                          var cost: Double, val info: QueryInfo) {
  final val resultNumber: Int = DPJoinPlan.getResultNumber
  final val tmpName: String = s"res$resultNumber"
  final val isJoin: Boolean = (left != null && right !=null)

  val toSQL: String = "Some SQL Text..."
  def explain() = println(this.print(""))
  def print(indent: String): String
  def getCost: Double
}

object DPJoinPlan{
  private var resultNumber = 0
  def getResultNumber: Int = {
    resultNumber += 1
    resultNumber
  }
}

/**
  * A table scan operation (A vertex in the graph)
  * @param table The table to be loaded
  * @param engine The engine which hosts the table
  * */
class Scan(val table: Vertex, override val engine: Engine, override val info: QueryInfo)
  extends DPJoinPlan(null, null, engine, 0, info){
  override def print(indent: String): String = s"$indent*$this (tID: ${table.id}) [${this.resultNumber}] [${getCost}] [$engine]"
  override def getCost: Double = engine.getQueryCost(this.toSQL)
}

/**
  * A Join between two [[DPJoinPlan]]s
  * @param left The left subplan
  * @param right The right subplan
  * @param vars The join keys
  * @param engine The engine in which the join will be executed
  * */
class Join(override val left: DPJoinPlan, override val right: DPJoinPlan, val vars: mutable.HashSet[Int],
                override val engine: Engine, override val info: QueryInfo)
  extends DPJoinPlan(null, null, engine, left.getCost + right.getCost, info){

  override def print(indent: String): String = s"${indent}" +
    s"Join(${this.getClass.getSimpleName})<${left.resultNumber}, ${right.resultNumber}> " +
    s"on ${vars} [$getCost] [$engine] [$resultNumber]" +
    s"\n${left.print(indent + "\t")}" +
    s"\n${right.print(indent + "\t")}"

  override def getCost: Double = left.getCost + right.getCost + engine.getQueryCost(this.toSQL)
}

/**
  * A Move of a [[DPJoinPlan]] to another [[Engine]]
  * @param dpJoinPlan The plan to be moved
  * @param engine The destination [[Engine]]
  * */
class Move(val dpJoinPlan: DPJoinPlan, override val engine: Engine, override val info: QueryInfo)
  extends DPJoinPlan(dpJoinPlan, null, engine, dpJoinPlan.getCost, info){

  def print(indent: String): String = s"${indent}Move(${this.getClass.getSimpleName})[$resultNumber] from ${dpJoinPlan.engine} " +
    s"to $engine cost $cost\n${dpJoinPlan.print(indent + "\t")}"
  def compareTo(o: DPJoinPlan): Int = cost.compareTo(o.getCost)
  def getCost: Double = dpJoinPlan.getCost + engine.getMoveCost(dpJoinPlan)
}