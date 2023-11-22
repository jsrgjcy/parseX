package com.sucx.core.parse.hive

import com.sucx.common.model.TableInfo
import com.sucx.core.{SparkDDLSQLParse, SqlParseUtil}

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

  val DDL_STR =
    """
      |CREATE TABLE dws.dws_bt4075(
      |     FV STRING   COMMENT 'FV'
      |    ,F1 STRING   COMMENT '日期' -- 日期注释
      |    ,F2 STRING   COMMENT '电池id'
      |    ,F3 STRING   COMMENT '起租状态'
      |    ,F4 STRING   COMMENT '当前所在事业部'
      |    ,F5 STRING   COMMENT '客户名称'
      |    ,F6 STRING   COMMENT '起租日期'
      |    ,F7 STRING   COMMENT '最后检测时间(tbox)'
      |    ,F8 BIGINT   COMMENT '离线时长-Tbox(单位天)'
      |    ,F9 DOUBLE   COMMENT '经度'
      |    ,F10 DOUBLE  COMMENT '纬度'
      |    ,F11 STRING  COMMENT '地理位置'
      |    ,F12 STRING  COMMENT '最后检测时间-station'
      |    ,F13 BIGINT  COMMENT '离线时长-station(单位天)'
      |    ,F14 STRING  COMMENT '最新换电站编码'
      |    ,F15 STRING  COMMENT '最新换电站名称'
      |    ,F16 STRING  COMMENT '最新换电站地址'
      |    ,F17 STRING  COMMENT '是否统计初电池'
      |    ,F18 STRING  COMMENT '最初事业部'
      |    ,F19 STRING  COMMENT 'tbox与station对比最终检测时间'
      |    ,F20 BIGINT  COMMENT 'tbox与station对比最终离线时长'
      |    ,ct STRING  COMMENT  '数据更新时间'
      |    ,it STRING  COMMENT  '运行日期'
      |)
      |COMMENT "电池摸排精细化跟踪日维度表"
      |STORED AS PARQUET
      |TBLPROPERTIES ("parquet.compress"="SNAPPY");
      |""".stripMargin

  //解析  action下的执行文件
  def parseHiveSQLTest = {
    val UT = "20220101"
    //    val path = "C:\\Persional\\workspace\\code\\cwp\\cwp-dwd\\src\\main\\resources\\lib\\ads\\ads_zk1080\\action\\build_zk1080.sql"
    //    val sql = Source.fromFile(path).mkString
    val sql = DDL_STR
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
    val sql = DDL_STR
    //    val res = SqlParseUtil.parseHiveSql(sql.replaceAll("@ut@", UT)).getOutputSets.toList
    val res = new SparkDDLSQLParse().parseDDL(sql.replaceAll("@ut@", UT))

    println(res.toString())

  }
}
