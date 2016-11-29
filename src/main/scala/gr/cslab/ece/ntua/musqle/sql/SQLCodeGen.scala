package gr.cslab.ece.ntua.musqle.sql

import gr.cslab.ece.ntua.musqle.plan.hypergraph.{DPJoinPlan, Join, Move}
import gr.cslab.ece.ntua.musqle.plan.spark._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.StringType

import scala.collection.mutable

class SQLCodeGen(val info: MQueryInfo) {

  def genSQL(scan: MuSQLEScan): String = {
    val tableName = matchTableName(scan.vertex.plan, this.info)
    val filter = makeCondition(scan.vertex.filter.condition)
    val projection = {
      if (!scan.projections.isEmpty) {
        scan.projections.reduceLeft(_ + ", " + _)
      }
      else { "*" }
    }

    val sql = s"""SELECT ${projection}
       |FROM ${scan.tmpName}
       |WHERE $filter""".stripMargin

    if (scan.isRoot) {
      val aggs = getAggregations()
    }

    sql
  }

  def genSQL(plan: MuSQLEJoin): String = {
    val subQueryTables = findJoinKeys(plan)
    val conditions = subQueryTables.map(key => info.idToCondition(key))
    val keys = subQueryTables.flatMap(key => info.idToCondition(key).references.map(_.asInstanceOf[AttributeReference]))
    val filters = findFiltersInSubQuery(plan)
    val vertices = keys.map(attribute => info.attributeToVertex.get(attribute.toString()).get)
    val names = findTableNames(plan)

    val projection = {
      if (!plan.projections.isEmpty) {
        plan.projections.reduceLeft(_ + ", " + _)
      }
      else { "*" }
    }
    val commaSeparatedNames = names.reduceLeft(_ + ", " + _)

    if (plan.isRoot) {
      val exp = getAggregateExpressions()
      plan.projections.clear()
      exp.foreach(plan.projections.add)
    }

    var SQL =
      s"""SELECT ${projection}
         |FROM $commaSeparatedNames""".stripMargin

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

    if (plan.isRoot) {
      val aggs = getAggregations()
      SQL += "\n"+aggs
    }

    SQL
  }

  private def getAggregateExpressions(): Set[String] = {
    var root = info.rootLogicalPlan
    while (root.children.size < 2) {
      root match {
        case agg: Aggregate => {
          if (agg.aggregateExpressions.size > 0) {
            return agg.aggregateExpressions.map(parseAggregateExpression).toSet
          }
        }
        case _ => {}
      }
      root = root.children(0)
    }

    Set.empty
  }

  private def parseAggregateExpression(expression: Expression): String = {
    expression match {
      case attRef: AttributeReference => attRef.toString.replace("#", "")
      case alias: Alias => {
        s"${parseAggregateExpression(alias.child)} AS ${alias.name}${alias.exprId.id}"
      }
      case divide: Divide => { s"${parseAggregateExpression(divide.left)}/${parseAggregateExpression(divide.right)}" }
      case multiply: Multiply => {s"${parseAggregateExpression(multiply.left)}*${parseAggregateExpression(multiply.right)}"}
      case subtract: Subtract => {s"${parseAggregateExpression(subtract.left)}-${parseAggregateExpression(subtract.right)}"}
      case declarativeAgg: DeclarativeAggregate => {
        declarativeAgg match {
          case sum: Sum => {s"SUM(${parseAggregateExpression(sum.child)})"}
          case avg: Average => {s"AVG(${parseAggregateExpression(avg.child)})"}
          case count: Count => {s"COUNT(${parseAggregateExpression(count.children(0))})"}
        }
      }
      case _ => {
        if (expression.children.size > 0)
          parseAggregateExpression(expression.children(0))
        else
          expression.toString()
      }
    }


  }

  private def getAggregations(): String = {
    var root = info.rootLogicalPlan
    var aggString = ""
    var groupBy = ""
    var orderBy = ""

    while (root.children.size < 2) {
      root match {
        case agg: Aggregate => {
          if (agg.aggregateExpressions.size > 0) {
          }
          if (agg.groupingExpressions.size > 0) {
            groupBy = "GROUP BY " +agg.groupingExpressions
                .map(exp => exp.toString()
                .replace("#", ""))
                .reduceLeft(_ + ", " + _)
          }
        }
        case sort: Sort => {
          orderBy = "ORDER BY "+sort.order.map(attr => attr.child.toString().replace("#", "")).reduceLeft(_ +", "+_)
        }
        case _ => {}
      }
      root = root.children(0)
    }

    aggString = groupBy

    if (!orderBy.isEmpty){
      if (!aggString.isEmpty) { aggString += "\n" }
      aggString += orderBy
    }

    aggString
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
              val key = ar.toString.replace("#","")
              key
            }
            case cast: Cast => {
              cast.child.toString.replace("#","")
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
              val key = ar.toString.replace("#","")
              key
            }
            case cast: Cast => {
              cast.child.toString.replace("#","")
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
            val key = lt.left.toString.replace("#","")
            key
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
              val key = lt.right.toString.replace("#","")
              key
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
              val key = gt.left.toString.replace("#","")
              key
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
              val key = gt.right.toString.replace("#","")
              key
            }
            case literal: Literal => {
              literal.dataType match {
                case StringType => s"""'${literal.value}'"""
                case _ => literal.value
              }
            }
          }
        }

        return s"$left > $right"
      }
      case ltoet: LessThanOrEqual => {
        val left = {
          ltoet.left match{
            case ar: AttributeReference =>{
              val id = info.attributeToVertex.get(ltoet.left.toString()).get.id
              val key = ltoet.left.toString.replace("#","")
              key
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
              val key = ltoet.right.toString.replace("#","")
              key
            }
            case literal: Literal => {
              literal.dataType match {
                case StringType => s"""'${literal.value}'"""
                case _ => literal.value
              }
            }
          }
        }

        return s"$left <= $right"
      }
      case gtoet: GreaterThanOrEqual => {
        val left = {
          gtoet.left match{
            case ar: AttributeReference =>{
              val id = info.attributeToVertex.get(gtoet.left.toString()).get.id
              val key = gtoet.left.toString.replace("#","")
              key
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
              val key = gtoet.right.toString.replace("#","")
              key
            }
            case literal: Literal => {
              literal.dataType match {
                case StringType => s"""'${literal.value}'"""
                case _ => literal.value
              }
            }
          }
        }

        return s"$left >= $right"
      }
      case and: And => makeCondition(and.left) + " AND " + makeCondition(and.right)
      case or: Or => makeCondition(or.left) + " OR " + makeCondition(or.right)
      case notNull: IsNotNull => {
        val attribute = notNull.child.asInstanceOf[AttributeReference]
        val id = info.attributeToVertex.get(attribute.toString()).get.id
        val key = attribute.toString.replace("#", "")
        s"$key IS NOT NULL"
      }
      case in: In => {
        val attribute = in.value.asInstanceOf[AttributeReference]
        val id = info.attributeToVertex.get(attribute.toString()).get.id
        val list = s"(${in.list.map(_.sql).reduceLeft(_+ ", " + _)})"
        val key = attribute.toString.replace("#", "")
        val result = s"$key IN $list"
        result
      }
      case _ => throw new UnsupportedOperationException(s"Operator: ${expr}")
    }
  }

  private def extractKey(expr: Expression): String = {
    expr match {
      case ar: AttributeReference => expr.toString.replace("#","")
      case cast: Cast => cast.child.toString.replace("#","")
      case literal: Literal => {
        literal.dataType match {
          case StringType => s"""'${literal.value}'"""
          case _ => literal.value.toString
        }
      }
    }

  }

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
        val n = matchTableName(scan.table.asInstanceOf[SparkPlanVertex].plan, this.info)
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

  def matchTableName(logicalRelation: LogicalRelation, info: MQueryInfo): String = {
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
