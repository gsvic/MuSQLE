package gr.cslab.ece.ntua.musqle.engine

import gr.cslab.ece.ntua.musqle.plan.hypergraph.DPJoinPlan

abstract class Engine(){
  override def equals(obj: scala.Any): Boolean = {
    this.getClass.equals(obj.getClass)
  }
  def getMoveCost(plan: DPJoinPlan): Double = {0.0}
  def getQueryCost(sql: String): Double = {
    println(s"\nQUERY=[$sql]\n")
    2.0}
}

case class Spark() extends Engine
case class Postgres() extends Engine

class HDFSFormat()
case class Json() extends HDFSFormat
case class Parquet() extends HDFSFormat

object Engine{
  val SPARK = Spark()
  val POSTGRES = Postgres()
}
