package gr.cslab.ece.ntua.musqle.engine

import gr.cslab.ece.ntua.musqle.plan.hypergraph.DPJoinPlan

abstract class Engine(){
  override def equals(obj: scala.Any): Boolean = {
    this.getClass.equals(obj.getClass)
  }
  def getMoveCost(plan: DPJoinPlan): Double = {
    println("MOVE "+this)
    0.0
  }
  def getQueryCost(sql: String): Double = {
    println("QUERY "+this)
    2.0}
}

case class Spark() extends Engine {
  /*override def getQueryCost(sql: String): Double  = {
    //TODO: Implement Spark SQL cost estimator module
    2.0
  }

  override def getMoveCost(plan: DPJoinPlan): Double = {
    println("Asking Spark for move")
    0.0
  }*/
}
case class Postgres() extends Engine

class HDFSFormat()
case class Json() extends HDFSFormat
case class Parquet() extends HDFSFormat

object Engine{
  val SPARK = Spark()
  val POSTGRES = Postgres()
}
