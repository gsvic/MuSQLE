package gr.cslab.ece.ntua.musqle.plan.spark

import java.util

import gr.cslab.ece.ntua.musqle.engine.Engine
import gr.cslab.ece.ntua.musqle.plan.hypergraph.Vertex
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.execution.datasources.LogicalRelation

/**
  * Created by vic on 18/10/2016.
  */
case class SparkPlanVertex(plan: LogicalRelation, override val engines: Seq[Engine],
                           filter: Filter = null, projections: scala.collection.mutable.HashSet[Attribute])
  extends Vertex(engines) {
  val connections = new util.ArrayList[(Int, Seq[Int])]()
}
