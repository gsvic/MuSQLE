package gr.cslab.ece.ntua.musqle.plan.hypergraph

import gr.cslab.ece.ntua.musqle.engine.Engine
import gr.cslab.ece.ntua.musqle.plan.spark.{MuSQLEJoin, MuSQLEScan}

import scala.collection.mutable

abstract class DPJoinPlan(val left: DPJoinPlan, val right: DPJoinPlan, val engine: Engine,
                          var cost: Double, val info: QueryInfo) {
  final val resultNumber: Int = DPJoinPlan.getResultNumber
  final val tmpName: String = s"mtpmres$resultNumber"
  final val isJoin: Boolean = (left != null && right !=null)
  var isRoot: Boolean = false
  var projections: mutable.HashSet[String] = new mutable.HashSet[String]

  def toSQL: String = "Some SQL Text..."
  def getRowsEstimation: Long = engine.getRowsEstimation(this)
  def explain() = println(this.print(""))
  def print(indent: String): String
  def getCost: Double

  def getInterResults(): mutable.HashSet[DPJoinPlan] = { getInterResults(this) }

  private def getInterResults(plan: DPJoinPlan): mutable.HashSet[DPJoinPlan] = {
    val s = mutable.HashSet[DPJoinPlan]()
    if (plan.isRoot) s.add(plan)
    plan match {
      case move: Move => mutable.HashSet(move)
      case join: Join => s.union(getInterResults(plan.left)).union(getInterResults(plan.right))
      case _ => mutable.HashSet.empty
    }
  }

  def toAbstract(): AbstractPlan = { AbstractPlan(getVertices(this), Seq()) }

  def getVertices(plan: DPJoinPlan): mutable.HashSet[String] = {
    plan match {
      case scan: MuSQLEScan => mutable.HashSet(scan.tableName)
      case join: MuSQLEJoin => getVertices(join.left) ++ getVertices(join.right)
      case move: Move => getVertices(move.left)
    }
  }
}

object DPJoinPlan{
  private var resultNumber = 0
  def zeroResultNumber: Unit = { resultNumber = 0}
  def getResultNumber: Int = {
    resultNumber += 1
    resultNumber
  }

  var totalGetCost = 0.0
}

/**
  * A table scan operation (A vertex in the graph)
  * @param table The table to be loaded
  * @param engine The engine which hosts the table
  * */
class Scan(val table: Vertex, override val engine: Engine, override val info: QueryInfo)
  extends DPJoinPlan(null, null, engine, 5, info){
  override def print(indent: String): String = s"$indent*Scan $this, " +
    s"Engine: [$engine], Cost: [${getCost}], [${this.tmpName}] "
  override def getCost: Double = engine.getCost(this)
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
    s"Join [${left.tmpName}, ${right.tmpName}] " +
    s"on ${vars} , Engine: [$engine], Cost: [$getCost], [$tmpName]" +
    s"\n${left.print(indent + "\t")}" +
    s"\n${right.print(indent + "\t")}"

  override def getCost: Double = {
    val start = System.currentTimeMillis()
    val cost = left.getCost + right.getCost + engine.getCost(this)
    val elapsed = System.currentTimeMillis() - start
    DPJoinPlan.totalGetCost += elapsed / 1000.0

    cost
  }
}

/**
  * A Move of a [[DPJoinPlan]] to another [[Engine]]
  * @param dpJoinPlan The plan to be moved
  * @param engine The destination [[Engine]]
  * */
class Move(val dpJoinPlan: DPJoinPlan, override val engine: Engine, override val info: QueryInfo)
  extends DPJoinPlan(dpJoinPlan, null, engine, dpJoinPlan.getCost, info){

  def print(indent: String): String = s"${indent}Move [${dpJoinPlan.tmpName}] from ${dpJoinPlan.engine} " +
    s"to $engine, Cost $cost [$tmpName]\n${dpJoinPlan.print(indent + "\t")}"
  def compareTo(o: DPJoinPlan): Int = cost.compareTo(o.getCost)
  def getCost: Double = dpJoinPlan.getCost + engine.getMoveCost(dpJoinPlan)

  override def getRowsEstimation: Long = left.engine.getRowsEstimation(left)
}