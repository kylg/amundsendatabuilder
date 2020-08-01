// Databricks notebook source
dbutils.widgets.text("today", "", "today")
dbutils.widgets.text("log_path", "", "log_path")
val today = dbutils.widgets.get("today")
val log_path = dbutils.widgets.get("log_path")

println(s"begin analyse task ${today}")

// COMMAND ----------

import org.apache.spark.sql.{DataFrame, SparkSession}

def refreshTableStatistics(spark: SparkSession, finishedTbls: Set[String], logFile: String) = {
  var finished = finishedTbls
  val dbs = spark.sharedState.externalCatalog.listDatabases()
  dbs.foreach(db => {
    println(s"========== begin analyse db ${db}")
    spark.sharedState.externalCatalog.listTables(db).foreach(tbl => {
      try {
        if (finished.contains(s"${db}.${tbl}")) {
          println(s"table ${db}.${tbl} is already done, so skip")
        } else {
          import org.apache.spark.sql.types.{DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType}
          val lst = collection.mutable.ArrayBuffer[String]()
          val tblObj = spark.sharedState.externalCatalog.getTable(db, tbl)
          tblObj.schema.fields.foreach(f => {
            if ((f.dataType == DoubleType || f.dataType == FloatType || f.dataType == IntegerType
              || f.dataType == LongType || f.dataType == ShortType || f.dataType == DecimalType)
              && !tblObj.partitionColumnNames.contains(f.name)) {
              lst.append(f.name)
            }
          })
          var action: String = ""
          val begin = System.currentTimeMillis()
          if (!lst.isEmpty) {
            val colStr = lst.map(t => s"`$t`").mkString(",")
            val sql = s"ANALYZE TABLE ${db}.${tbl} COMPUTE STATISTICS FOR COLUMNS $colStr"
            println(sql)
            spark.sql(sql)
            println(s"finish analyse table ${db}.${tbl}")
            action = "analysed"
          } else {
            println(s"table ${db}.${tbl} has no numberic columns, so skip")
            action = "skipped"
          }
          val end = System.currentTimeMillis()
          finished = finished.+(s"${db}.${tbl}")
          import java.text.SimpleDateFormat
          import java.util.Date
          import java.text.SimpleDateFormat
          val f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val beginStr = f.format(new Date(begin))
          val endStr = f.format(new Date(end))

          val logStr = s"${db},${tbl},${action},${beginStr},${endStr},${(end - begin) / 1000}\n"
          appendLog(logStr, logFile)
        }
      } catch {
        case x: Exception => {
          println(s"============error analyse table: ${db}.${tbl}")
          println(x)
        }
      }
    })
  })
}

def getFile(logFile:String)={
  import java.io.File
  val f = if(logFile.startsWith("dbfs:")){
    new File(logFile.replace("dbfs:", "/dbfs"))
  }else {
    new File(logFile)
  }
  f
}

def appendLog(logStr: String, logFile: String): Unit = {
  import java.io.{FileWriter, PrintWriter}
  var writer: PrintWriter = null
  try {
    val fileWriter = new FileWriter(getFile(logFile), true) //Set true for append mode
    val writer = new PrintWriter(fileWriter)
    writer.append(logStr)
    writer.flush()
    writer.close()
  } catch {
    case x: Exception => {
      println(x)
      if (writer != null) {
        writer.close()
      }
    }
  }
}

def loadHistoricalLogs(spark: SparkSession, logFile: String) = {
  import java.io.FileNotFoundException
  try {
    dbutils.fs.ls(logFile)
    val tblList = spark.read.option("header", true).csv(logFile).collect().map(r => {
      val db: String = r.getAs("db")
      val table: String = r.getAs("table")
      s"${db}.${table}"
    })
    Set(tblList: _*)
  } catch {
    case x: FileNotFoundException => {
      println(s"log file not exist: $logFile")
      val f = getFile(logFile)
      f.createNewFile()
      val header = "db,table,action,begin,end,duration"
      appendLog(header, logFile)
      Set[String]()
    }
  }
}

// COMMAND ----------
val logFile = s"${log_path}/${today}.csv"
val tblSet = loadHistoricalLogs(spark, logFile)

refreshTableStatistics(spark, tblSet, logFile)
