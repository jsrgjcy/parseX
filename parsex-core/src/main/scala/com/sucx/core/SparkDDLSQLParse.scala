package com.sucx.core

import com.sucx.common.enums.OperatorType
import com.sucx.common.model.TableInfo
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.datasources.CreateTable
import org.apache.spark.sql.internal.SQLConf

import java.util.{HashSet => JSet}
import scala.collection.JavaConversions._

class SparkDDLSQLParse extends AbstractSqlParse {

  /**
   * 抽象解析
   *
   * @param sqlText sql
   * @return tuple4
   * @throws SqlParseException
   */
  override protected def parseInternal(sqlText: String): (JSet[TableInfo], JSet[TableInfo], JSet[TableInfo]) = ???

  def parseDDL(sqlText: String): List[TableInfo] = {
    splitSql(sqlText).map(sql => logicalPlanToTableInfo(sql)).toList
  }

  def logicalPlanToTableInfo(sqlText: String): TableInfo = {
    val parser = new SparkSqlParser(new SQLConf)
    val logicalPlan: LogicalPlan = try {
      parser.parsePlan(sqlText)
    } catch {
      case _ => throw new Exception(" 不是DDL语句或语法错误  ")
    }

    logicalPlan match {
      case plan: CreateTable =>
        val project: CreateTable = plan.asInstanceOf[CreateTable]
        val columnsSet = new JSet[FieldSchema]()
        println(project.toJSON)

        project.tableDesc.schema.fields.foreach(field => {
          columnsSet.add(new FieldSchema(field.name, field.dataType.typeName, field.getComment().get))
        })
        val tableIdentifier: TableIdentifier = project.tableDesc.identifier
        val ti = new TableInfo(tableIdentifier.table, tableIdentifier.database.getOrElse(this.currentDb), OperatorType.CREATE, columnsSet)
        ti.setTableComment(project.tableDesc.comment.get)
        ti
      case _ => throw new Exception(" 不是DDL语句或语法错误 ")
    }
  }
}
