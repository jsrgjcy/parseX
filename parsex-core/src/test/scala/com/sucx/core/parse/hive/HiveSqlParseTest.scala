package com.sucx.core.parse.hive

import com.sucx.core.SqlParseUtil

import scala.collection.JavaConversions.asScalaSet
import scala.collection.immutable.ListSet
import scala.io.Source

/*
*
* add by jucongying 20220524
*
* */
object HiveSqlParseTest {
  def main(args: Array[String]): Unit = {
    parseHiveDDLTest
    //    parseHiveSQLTest
  }

  // 扫描项目文件下的所有代码的
  def scanCodes(): Unit = {
    // 指定文件路径
    // 扫描action下的.sql文件
    // 解析sql语法 得到输入输出
    // todo
  }

  //解析  action下的执行文件
  def parseHiveSQLTest = {
    val UT = "20220101"
    val path = "C:\\Persional\\workspace\\code\\cwp\\cwp-dwd\\src\\main\\resources\\lib\\ads\\ads_zk1080\\action\\build_zk1080.sql"
    val sql = Source.fromFile(path).mkString
    val res = SqlParseUtil.parseHiveSql(sql.replaceAll("@ut@", UT))
    val inputTables = res.getInputSets.to[ListSet]
    val sourceTables = inputTables.filter(i => i.getDbName != "tmp")
    val inputTempTables = inputTables.filter(i => i.getDbName == "tmp")
    val outputTables = res.getOutputSets.to[ListSet]
    val targetTables = outputTables.filter(i => i.getDbName != "tmp")
    val outputTempTables = outputTables.filter(i => i.getDbName == "tmp")
    println("输入表 : " + sourceTables.map(_.toString.replaceAll(UT, "@ut@")).mkString("|"))
    println("输出表 : " + targetTables.map(_.toString.replaceAll(UT, "@ut@")).mkString("|"))
    println("中间表 : " + ListSet(inputTempTables, outputTempTables).map(_.toString.replaceAll(UT, "@ut@")).mkString("|"))
  }

  // 解析DDL
  def parseHiveDDLTest = {
    val UT = "20220101"
    val path = "C:\\Persional\\workspace\\code\\cwp\\cwp-dwd\\src\\main\\resources\\lib\\ads\\ads_zk1080\\schema\\zk1080.sql"
    val sql = Source.fromFile(path).mkString
    val res = SqlParseUtil.parseHiveSql(sql.replaceAll("@ut@", UT))
    println(res)
    res.getOutputSets.foreach(t =>
      t.getColumns.toList.sortBy(c => c.getName.replaceAll("F", "").toInt).map(println(_))
    )
  }
}
