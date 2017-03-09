package gr.cslab.ece.ntua.musqle.plan.hypergraph

import scala.collection.mutable.HashSet

/**
  * Created by vic on 9/3/2017.
  */
case class AbstractPlan(val vertices: HashSet[String], val edges: Seq[String]) {
  override def equals(o: Any): Boolean = { vertices.equals(o.asInstanceOf[AbstractPlan].vertices) }
}