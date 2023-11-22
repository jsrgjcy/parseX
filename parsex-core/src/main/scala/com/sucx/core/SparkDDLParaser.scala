package com.sucx.core

import com.sucx.common.enums.OperatorType
import com.sucx.common.model.TableInfo
import org.apache.hadoop.hive.metastore.api.FieldSchema

import scala.collection.JavaConverters._

object SparkDDLParaser {
  def main(args: Array[String]): Unit = {
    val sql =
      """
        |CREATE TABLE cwp_ads.ads_bt4046 ( -- 表注释字符
        |   FV VARCHAR(64)    -- rowkey:充电机_电池_充电时辰_创建时间
        |  ,F1 VARCHAR(64)    -- rowkey:充电机_电池_充电时辰
        |  ,F2 VARCHAR(32)    -- 充电机编号
        |  ,F3 VARCHAR(32)    -- 电池id
        |  ,F4 VARCHAR(32)    -- 开始时间
        |  ,F5 VARCHAR(32)    -- 结束时间
        |  ,F6 INT            -- 开始SOC
        |  ,F7 INT            -- 结束SOC
        |  ,F8  VARCHAR(16)   -- 充电电量
        |  ,F9  VARCHAR(32)   -- 创建时间
        |  ,F10 VARCHAR(32)   -- 换电站编号
        |  ,F11 VARCHAR(64)   -- 换电站名称
        |  ,F12 VARCHAR(32)   -- 事业部
        |  ,F13 VARCHAR(16)   -- 最高单体电压（V）
        |  ,F14 INT           -- 最高单体电压组号
        |  ,F15 VARCHAR(16)   -- 最高单体温度（摄氏度）
        |  ,F16 INT           -- 最高单体温度组号
        |  ,F17 VARCHAR(16)   -- 最低单体温度（摄氏度）
        |  ,F18 INT           -- 最低单体温度组号
        |  ,F19 VARCHAR(32)   -- source: 'charge_result' \ 'station'
        |  ,F20 INT           -- 充电选择:0换电 1充电枪
        |  ,F21 INT           --电池仓位
        |  ,F22 DOUBLE        --等效容量
        |  ,F23 DOUBLE        --等效容量-均值
        |  ,F24 DOUBLE        --等效容量-最大值
        |  ,F25 DOUBLE        --等效容量-最小值
        |  ,F26 DOUBLE        --等效容量-标准差
        |  ,F27 INT           --等效容量-数量
        |  ,PRIMARY KEY (`FV`)
        |) ENGINE=InnoDB DEFAULT CHARSET=utf8
        |;
        |""".stripMargin

    //    TableInfo

    val res = parseTableSchema(sql)
    println(res)
  }

  def parseTableSchema(sql: String): TableInfo = {
    val x = sql.split(System.lineSeparator()).filter(l => l.toUpperCase().contains("CREATE"))
    val c = x(0).trim.replaceAll("\\s+|\\t+", " ")

    val ca = c.split("--")
    val tableAnnotation = if (ca.length > 1) { // 有注释
      ca(1).trim
    } else ""

    val cc = c.split(" ")
    val (db, table) = if (cc(2).trim.contains(".")) {
      val ccc = cc(2).split("\\.")
      val db = ccc(0).trim
      val table = ccc(1).trim
      (db, table)
    } else {
      ("", cc(2).trim)
    }
    val tableName = cc(2)
    // todo 表注释
    val res = new TableInfo(db, tableName, OperatorType.CREATE, parseFieldSchema(sql).asJava)
    res.setTableComment(tableAnnotation)
    res
  }

  def parseFieldSchema(sql: String): Set[FieldSchema] = {
    sql.split(System.lineSeparator()).map(l => {
      val lStr = l.trim.replaceAll("\\s+|\\t+", " ")
      if (lStr.trim == "" || lStr.trim == ";") {
        null
      } else {
        val arr = lStr.split("--")

        val (fieldName, fieldType, fieldComm) = if (arr.length > 1) {
          val a0 = arr(0).trim // 语句
          val a1 = arr(1).trim // 注释文字
          if (a0.toLowerCase().contains("comment")) { // 如果有comment语句
            val a00 = a0.replaceAll(",", "") // 去除逗号
            val al = a00.split(" ")
            // comment 提取 todo
            (al(0), al(1), "" + arr(1))
          } else { //  没有comment
            // if (a0.contains(","))
            val a00 = a0.replaceAll(",", "") // 去除逗号
            val al = a00.split(" ")
            (al(0), al(1), arr(1))
          }
        } else { // 没有注释语句
          val a0 = arr(0).trim // 语句
          if (a0.toLowerCase().contains("comment")) { // 如果有comment语句
            val a00 = a0.replaceAll(",", "") // 去除逗号
            val al = a00.split(" ")
            // 识别comment和注释 合并一起 todo
            (al(0), al(1), "") // 从comment来
          } else {
            val a00 = a0.replaceAll(",", "") // 去除逗号
            val al = a00.split(" ")
            (al(0), al(1), "") // 从comment来
          }
        }
        if (List("CREATE", "PRIMARY", ")").contains(fieldName.toUpperCase())) {
          null
        } else {
          // 识别comment和注释 合并一起 todo
          new FieldSchema(fieldName, fieldType, fieldComm)
        }
      }
    }).filter(l => l != null).toSet
  }

}
