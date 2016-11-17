package gr.cslab.ece.ntua.musqle.plan.spark

import gr.cslab.ece.ntua.musqle.engine.Engine
import gr.cslab.ece.ntua.musqle.plan.hypergraph.Scan
import gr.cslab.ece.ntua.musqle.sql.SQLCodeGen
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

case class MuSQLEScan(val vertex: SparkPlanVertex, override val engine: Engine, override val info: MQueryInfo)
  extends Scan(vertex, engine, info){

  val codeGen = new SQLCodeGen(info)

  override val toSQL: String = {
    codeGen.genSQL(this)
  }

  val tableName = codeGen.matchTableName(vertex.plan, info)
  val projection = vertex.plan.output.map(attr => s"${attr.name} ${attr.toString.replace("#", "")}")
    .reduceLeft(_ +", "+ _)

  engine.createView(this, tableName, projection)

  //this.vertex.plan.attributeMap.foreach(attr => info.attributeToRelName.put(attr.toString(), this.tmpName))


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
    str += s" Filter[${vertex.filter.condition.children}]"

    str
  }
}
