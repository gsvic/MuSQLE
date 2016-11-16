package gr.cslab.ece.ntua.musqle.sql

import gr.cslab.ece.ntua.musqle.cost.Scan
import gr.cslab.ece.ntua.musqle.plan.hypergraph.{DPJoinPlan, Join, Move}
import gr.cslab.ece.ntua.musqle.plan.spark._
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, In, IsNotNull, LessThan, LessThanOrEqual, Literal, Or}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.StringType

import scala.collection.mutable

class SQLCodeGen(val info: MQueryInfo) {

  def genSQL(scan: MuSQLEScan): String = {
    val tableName = matchTableName(scan.vertex.plan)
    val filter = makeCondition(scan.vertex.filter.condition)

    val projection = scan.vertex.plan.output.map(attr => s"${attr.name} AS ${attr.toString.replace("#", "")}")
      .reduceLeft(_ +", "+ _)

    val sql = s"""SELECT *
       |FROM $tableName t${scan.vertex.id}
       |WHERE $filter""".stripMargin

    sql
  }

  def genSQL(plan: MuSQLEJoin): String = {
    val subQueryTables = findJoinKeys(plan)
    val conditions = subQueryTables.map(key => info.idToCondition(key))
    val keys = subQueryTables.flatMap(key => info.idToCondition(key).references.map(_.asInstanceOf[AttributeReference]))
    val filters = findFiltersInSubQuery(plan)
    val tables = keys.map{ attribute =>
      matchTableName(info.attributeToVertex.get(attribute.toString()).get.plan)
    }
    val names = findTableNames(plan)

    //val projections = getProjections(plan)

    val commaSeperatedNames = names.reduceLeft(_ + ", " + _)
    var SQL =
      s"""SELECT *
         |FROM """.stripMargin

    SQL += names.toList(0)

    names.toList.slice(1, tables.size).foreach{table =>
      SQL += s""", $table"""
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

  private def getSparkPlanProjections(plan: LogicalPlan): mutable.HashSet[String] = {
    val projections = new mutable.HashSet[String]()

    if (plan.children.size == 1){
      plan.output.foreach(att => projections.add(att.toString()))
      getSparkPlanProjections(plan.children(0)).foreach(projections.add)
    }
    projections
  }

  private def getProjections(plan: DPJoinPlan): mutable.HashSet[String] = {
    val sparkPlanProjections = getSparkPlanProjections(plan.info.asInstanceOf[MQueryInfo].rootLogicalPlan)

    sparkPlanProjections
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
      case in: In => {
        val attribute = in.value.asInstanceOf[AttributeReference]
        val id = info.attributeToVertex.get(attribute.toString()).get.id
        val list = s"(${in.list.map(_.sql).reduceLeft(_+ ", " + _)})"
        val result = s"t$id.${attribute.name} IN $list"
        result
      }
      case _ => throw new UnsupportedOperationException(s"Operator: ${expr}")
    }
  }

  private def parseExpression(){}

  /**
    * @return A [[mutable.HashSet]] with the tables contained in the input subquery
    * */
  private def findJoinKeys(plan: DPJoinPlan): mutable.HashSet[Integer] ={
    val hashSet = new mutable.HashSet[Integer]()
    plan match {
      case join: Join => {
        join.vars.foreach(x => hashSet.add(x))
        findJoinKeys(plan.left).foreach(hashSet.add)
        findJoinKeys(plan.right).foreach(hashSet.add)
      }
      case _ => {}
    }
    hashSet
  }

  private def findTableNames(plan: DPJoinPlan): mutable.HashSet[String] = {
    val names = new mutable.HashSet[String]()

    plan match {
      case join: MuSQLEJoin => {
        findTableNames(join.left).foreach(names.add)
        findTableNames(join.right).foreach(names.add)
      }
      case move: MuSQLEMove => {
        names.add(move.tmpName)
      }
      case scan: MuSQLEScan => {
        val n = matchTableName(scan.table.asInstanceOf[SparkPlanVertex].plan)
        names.add(scan.tmpName)
      }
    }
    names
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
      case move: Move => {}
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
