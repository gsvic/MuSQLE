package gr.cslab.ece.ntua.musqle.plan.spark

import gr.cslab.ece.ntua.musqle.engine.Engine
import gr.cslab.ece.ntua.musqle.plan.hypergraph.Scan
import gr.cslab.ece.ntua.musqle.sql.SQLCodeGen
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

case class MuSQLEScan(val vertex: SparkPlanVertex, override val engine: Engine, override val info: MQueryInfo)
  extends Scan(vertex, engine, info){
  val codeGen = new SQLCodeGen(info)

  val tableName = codeGen.matchTableName(vertex.plan, info)
  vertex.plan.output.map(attr => attr.toString.replace("#", "")).foreach(this.projections.add)
  val projection = vertex.plan.output
    .map(attr => s"${attr.name} ${attr.toString.replace("#", "")}")
    .reduceLeft(_ +", "+ _)

  override def toSQL: String = {
    codeGen.genSQL(this)
  }

  engine.createView(this, tableName, projection)



  override def toString: String = {
    var str = "MuSQLEScan: "
    this.vertex.plan match {
      case lr: LogicalRelation => {
        lr.relation match {
          case hdfs: HadoopFsRelation => {
            str += s"${lr.relation.asInstanceOf[HadoopFsRelation].location.paths(0)}"
          }
          case _ => {
            str += lr.relation.schema
          }
        }
      }
      case _ => {str += vertex.toString}
    }
    str += s" Filter[${vertex.filter.condition.sql}]"

    str
  }
}
