package gr.cslab.ece.ntua.musqle.sql

import gr.cslab.ece.ntua.musqle.plan.hypergraph.{DPJoinPlan, Join}
import gr.cslab.ece.ntua.musqle.plan.spark.{MQueryInfo, MuSQLEJoin, MuSQLEScan}
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo,
  Expression, GreaterThan, GreaterThanOrEqual, IsNotNull, LessThan, LessThanOrEqual, Literal, Or}
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.StringType

import scala.collection.mutable

class SQLCodeGen(val info: MQueryInfo) {

  def genSQL(scan: MuSQLEScan): String = {
    val tableName = matchTableName(scan.vertex.plan)
    val filter = makeCondition(scan.vertex.filter.condition)

    s"""SELECT *
       |FROM $tableName t${scan.vertex.id}
       |WHERE $filter""".stripMargin
  }

  def genSQL(plan: MuSQLEJoin): String = {
    val conditions = findTablesInSubQuery(plan).map(key => info.idToCondition(key))
    val keys = findTablesInSubQuery(plan).flatMap(key => info.idToCondition(key).references.map(_.asInstanceOf[AttributeReference]))
    val filters = findFiltersInSubQuery(plan)
    val tables = keys.map{ attribute =>
      info.attributeToVertex.get(attribute.toString()).get
    }

    var SQL =
      s"""SELECT *
         |FROM """.stripMargin

    SQL += {
      val vertex = tables.toList(0)
      val name = matchTableName(vertex.plan)
      val id = vertex.id

      s"$name t$id"
    }

    tables.toList.slice(1, tables.size).foreach{table =>
      val name = matchTableName(table.plan)
      SQL += s""", $name t${table.id}"""
    }

    var WHERE = ""

    if (conditions.size > 0) {
      WHERE += "\nWHERE " + conditions.map(makeCondition).reduce(_ + "\nAND " + _)
    }

    if (filters.size > 0) {
      val f =  filters.map(f => makeCondition(f.condition)).reduce(_ + "\nAND " + _)
      if (WHERE.equals("")) {
        WHERE += "\nWHERE " + f
      }
      else {
        WHERE += "\nAND " + f
      }
    }

    SQL += WHERE
    SQL
  }

  private def makeCondition(expr: Expression): String ={
    expr match {
      case eq: EqualTo => {
        val left = {
          eq.left match{
            case ar: AttributeReference =>{
              val id = info.attributeToVertex.get(eq.left.toString()).get.id
              val key = eq.left.toString().split("#")(0)
              s"t$id.$key"
            }
            case literal: Literal => {
              literal.dataType match {
                case StringType => s"""'${literal.value}'"""
                case _ => literal.value
              }
            }

          }
        }

        val right = {
          eq.right match{
            case ar: AttributeReference =>{
              val id = info.attributeToVertex.get(eq.right.toString()).get.id
              val key = eq.right.toString().split("#")(0)
              s"t$id.$key"
            }
            case literal: Literal => {
              literal.dataType match {
                case StringType => s"""'${literal.value}'"""
                case _ => literal.value
              }
            }
          }
        }

        return s"$left = $right"
      }
      case lt: LessThan => {
        val left = {
        lt.left match{
          case ar: AttributeReference =>{
            val id = info.attributeToVertex.get(lt.left.toString()).get.id
            val key = lt.left.toString().split("#")(0)
            s"t$id.$key"
          }
          case literal: Literal => {
            literal.dataType match {
              case StringType => s"""'${literal.value}'"""
              case _ => literal.value
            }
          }
        }
      }

        val right = {
          lt.right match{
            case ar: AttributeReference =>{
              val id = info.attributeToVertex.get(lt.right.toString()).get.id
              val key = lt.right.toString().split("#")(0)
              s"t$id.$key"
            }
            case literal: Literal => {
              literal.dataType match {
                case StringType => s"""'${literal.value}'"""
                case _ => literal.value
              }
            }
          }
        }

        return s"$left < $right"}

      case gt: GreaterThan => {
        val left = {
          gt.left match{
            case ar: AttributeReference =>{
              val id = info.attributeToVertex.get(gt.left.toString()).get.id
              val key = gt.left.toString().split("#")(0)
              s"t$id.$key"
            }
            case literal: Literal => {
              literal.dataType match {
                case StringType => s"""'${literal.value}'"""
                case _ => literal.value
              }
            }
          }
        }

        val right = {
          gt.right match{
            case ar: AttributeReference =>{
              val id = info.attributeToVertex.get(gt.right.toString()).get.id
              val key = gt.right.toString().split("#")(0)
              s"t$id.$key"
            }
            case literal: Literal => {
              literal.dataType match {
                case StringType => s"""'${literal.value}'"""
                case _ => literal.value
              }
            }
          }
        }

        return s"$left < $right"
      }
      case ltoet: LessThanOrEqual => {
        val left = {
          ltoet.left match{
            case ar: AttributeReference =>{
              val id = info.attributeToVertex.get(ltoet.left.toString()).get.id
              val key = ltoet.left.toString().split("#")(0)
              s"t$id.$key"
            }
            case literal: Literal => {
              literal.dataType match {
                case StringType => s"""'${literal.value}'"""
                case _ => literal.value
              }
            }
          }
        }

        val right = {
          ltoet.right match{
            case ar: AttributeReference =>{
              val id = info.attributeToVertex.get(ltoet.right.toString()).get.id
              val key = ltoet.right.toString().split("#")(0)
              s"t$id.$key"
            }
            case literal: Literal => {
              literal.dataType match {
                case StringType => s"""'${literal.value}'"""
                case _ => literal.value
              }
            }
          }
        }

        return s"$left < $right"
      }
      case gtoet: GreaterThanOrEqual => {
        val left = {
          gtoet.left match{
            case ar: AttributeReference =>{
              val id = info.attributeToVertex.get(gtoet.left.toString()).get.id
              val key = gtoet.left.toString().split("#")(0)
              s"t$id.$key"
            }
            case literal: Literal => {
              literal.dataType match {
                case StringType => s"""'${literal.value}'"""
                case _ => literal.value
              }
            }
          }
        }

        val right = {
          gtoet.right match{
            case ar: AttributeReference =>{
              val id = info.attributeToVertex.get(gtoet.right.toString()).get.id
              val key = gtoet.right.toString().split("#")(0)
              s"t$id.$key"
            }
            case literal: Literal => {
              literal.dataType match {
                case StringType => s"""'${literal.value}'"""
                case _ => literal.value
              }
            }
          }
        }

        return s"$left < $right"
      }
      case and: And => makeCondition(and.left) + " AND " + makeCondition(and.right)
      case or: Or => makeCondition(or.left) + " OR " + makeCondition(or.right)
      case notNull: IsNotNull => {
        val attribute = notNull.child.asInstanceOf[AttributeReference]
        val id = info.attributeToVertex.get(attribute.toString()).get.id
        val key = attribute.name
        s"t$id.$key IS NOT NULL"
      }
      case _ => throw new UnsupportedOperationException()
    }
  }

  private def parseExpression(){}

  /**
    * @return A [[mutable.HashSet]] with the tables contained in the input subquery
    * */
  private def findTablesInSubQuery(plan: DPJoinPlan): mutable.HashSet[Integer] ={
    val hashSet = new mutable.HashSet[Integer]()
    plan match {
      case join: Join => {
        join.vars.foreach(x => hashSet.add(x))
        findTablesInSubQuery(plan.left).foreach(hashSet.add)
        findTablesInSubQuery(plan.right).foreach(hashSet.add)
      }
      case _ => {}
    }
    hashSet
  }

  private def findFiltersInSubQuery(plan: DPJoinPlan): mutable.HashSet[Filter] ={
    val hashSet = new mutable.HashSet[Filter]()

    plan match{
      case join: MuSQLEJoin => {
        findFiltersInSubQuery(plan.left).foreach(hashSet.add)
        findFiltersInSubQuery(plan.right).foreach(hashSet.add)
      }
      case scan: MuSQLEScan => {
        val filter = scan.vertex.filter
        if (filter != null) {
          hashSet.add(filter)
        }
      }
      case _ => throw new Exception()
    }
    hashSet
  }

  private def matchTableName(logicalRelation: LogicalRelation): String = {
    val candidateTableAttributes = logicalRelation.attributeMap.map(_._2.name)
    info.planToTableName.foreach{ plan =>
      var flag = true
      val tmpTableAttributes = plan._1.attributeMap.map(_._2.name)
      for (attr <- tmpTableAttributes){
        if (!candidateTableAttributes.toSeq.contains(attr)){
          flag = false
        }
      }
      if (flag) return plan._2
    }
    throw new Exception("Cannot find matching table")
  }
}
