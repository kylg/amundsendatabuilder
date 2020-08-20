// Databricks notebook source
dbutils.widgets.text("today", "", "today")
dbutils.widgets.text("preview_rows", "", "preview_rows")
dbutils.widgets.text("csv_path", "", "csv_path")
val today = dbutils.widgets.get("today")
val csv_path = dbutils.widgets.get("csv_path")
val preview_rows = dbutils.widgets.get("preview_rows")

val rowCount:Int = if(preview_rows!=null&&preview_rows.length>0){
  val pr = preview_rows.toInt
  if(pr>50)50
  else pr
}else{
  10
}

println(s"begin analyse task ${today}")

// COMMAND ----------

import org.apache.spark.sql.{DataFrame, SparkSession}

def refreshTableStatistics(spark: SparkSession, csvPath:String, previewRowCount:Int) = {
  val dbs = spark.sharedState.externalCatalog.listDatabases()
  dbs.foreach(db => {
    println(s"========== begin analyse db ${db}")
    spark.sharedState.externalCatalog.listTables(db).foreach(tbl => {
      try {
        if (checkCsvFileExist(db, tbl, csvPath)) {
          println(s"table ${db}.${tbl} is already done, so skip")
        } else {
          statisticsTbl(spark, db, tbl)
          exportSampleData(spark, db, tbl, csvPath, previewRowCount)
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

def statisticsTbl(spark:SparkSession, db:String, tbl:String) = {
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
  if (!lst.isEmpty) {
    val begin = System.currentTimeMillis()
    val colStr = lst.map(t => s"`$t`").mkString(",")
    val sql = s"ANALYZE TABLE ${db}.${tbl} COMPUTE STATISTICS FOR COLUMNS $colStr"
    println(sql)
    spark.sql(sql)
    val end = System.currentTimeMillis()
    println(s"finish analyse table ${db}.${tbl}, it takes ${(end-begin)/1000} seconds")
  } else {
    println(s"table ${db}.${tbl} has no numberic columns, so skip")
  }
}

def exportSampleData(spark:SparkSession, db:String, tbl:String, csvBase:String, previewRowCount:Int)={
  val sql = s"select * from ${db}.${tbl} limit ${previewRowCount}"
  val path = s"${csvBase}/tmp/${db}.${tbl}"
  spark.sql(sql).coalesce(1).write.option("header", true).csv(path)
  // move the csv file only
  dbutils.fs.ls(path).foreach(p=>{
    // only move csv file to target
    if(p.path.endsWith(".csv")){
      val csvPath = getCsvFilePath(db, tbl, csvBase)
      dbutils.fs.mv(p.path, csvPath)
      println(s"exported to the csv file $csvPath")
    }
  })
  // remove the tmp dir
  dbutils.fs.rm(s"${csvBase}/tmp/${db}.${tbl}", true)
}

def getCsvFilePath(db:String, tbl:String, csvBase:String)={
  s"${csvBase}/output/${db}.${tbl}.csv"
}

def checkCsvFileExist(db:String, tbl:String, csvBase:String) = {
  val csvFile: String = getCsvFilePath(db, tbl, csvBase)
  import java.io.FileNotFoundException
  try {
    dbutils.fs.ls(csvFile)
    true
  } catch {
    case x: FileNotFoundException => {
      println(s"csv file not exist: $csvFile")
      false
    }
  }
}

// COMMAND ----------
val csvPath = s"${csv_path}/${today}"
refreshTableStatistics(spark, csvPath, rowCount)
