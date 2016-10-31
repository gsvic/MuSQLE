package gr.cslab.ece.ntua.musqle.plan.spark

import gr.cslab.ece.ntua.musqle.engine._
import gr.cslab.ece.ntua.musqle.plan.hypergraph.{DPJoinPlan, Join}
import gr.cslab.ece.ntua.musqle.sql.SQLCodeGen
import org.apache.spark.sql.catalyst.expressions

import scala.collection.mutable

class MuSQLEJoin(override val left: DPJoinPlan, override val right: DPJoinPlan,
                 override val vars: mutable.HashSet[Int], override val engine: Engine, override val info: MQueryInfo)
  extends Join(left, right, vars, engine, info){

  override val toSQL: String = {
    val codeGenerator = new SQLCodeGen(info)
    codeGenerator.genSQL(this)
  }


}
