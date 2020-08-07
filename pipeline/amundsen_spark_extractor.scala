// Databricks notebook source
dbutils.widgets.text("today", "", "today")
dbutils.widgets.text("export_path", "", "export_path")
dbutils.widgets.text("csv_path", "", "csv_path")
dbutils.widgets.text("db_groups", "", "db_groups")
val today = dbutils.widgets.get("today")
val export_path = dbutils.widgets.get("export_path")
val copy_csv_path = dbutils.widgets.get("export_csv_path")
val db_groups = dbutils.widgets.get("db_groups")
val export_file_path = s"${export_path}/${today}"


// COMMAND ----------

// val databases = spark.sharedState.externalCatalog.listDatabases()
// spark.sharedState.externalCatalog.listTables("platform_test")

import org.apache.spark.sql.catalyst.catalog.{CatalogStatistics, CatalogTable}
import org.apache.spark.sql.{DataFrame, SparkSession}

case class ColumnDef(tblId: String, dbName: String, tblName: String, tblType: String, tblDesc: String, tblLocation: String,
                     colName: String, colSortOrder: Int, colType: String, colDesc: String, isPartition: Boolean,
                     isView: Boolean, compressType: String, lastUpdateTime: Long, pipeline:String, source:String, p0Name: String, p0Time: String, p1Name: String,
                     p1Time: String, nullCount:String, distinctCount:String, max:String, min:String, avgLen:String, maxLen:String)

def loadTableDefs(spark: SparkSession, groupNum:Int): Iterator[(DataFrame, Int)] = {
  val dbs = spark.sharedState.externalCatalog.listDatabases()
  val dbGroups = dbs.zipWithIndex.groupBy(db_index => {db_index._2%groupNum}).map(g => {(g._1, g._2.map(db_index=>db_index._1))})

  dbGroups.map(groupPair => {
    val columnDefList = groupPair._2.flatMap(db => {
      loadTblDefsByDb(spark, db)
    })
    (spark.createDataFrame(columnDefList), groupPair._1)
  }).iterator
}

def loadTblDefsByDb(spark: SparkSession, db:String) = {
  import java.net.URI
  import java.text.SimpleDateFormat
  import java.util.Date
  spark.sharedState.externalCatalog.listTables(db).flatMap(tbl => {
    try {
      val tblObj = spark.sharedState.externalCatalog.getTable(db, tbl)
      println("============get table info for " + tbl)

      val locationUri: URI = tblObj.storage.locationUri.getOrElse(null)
      val locationStr = if (locationUri != null) locationUri.toString else ""
      var p0_name: String = ""
      var p0_time: String = ""
      var p1_name: String = ""
      var p1_time: String = ""
      if (!"VIEW".equals(tblObj.tableType.name)) {
        if (!spark.sharedState.externalCatalog.listPartitionNames(db, tbl).isEmpty) {
          val ps = spark.sharedState.externalCatalog.listPartitions(db, tbl)
          if (ps.size > 0) {
            val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            p0_name = ps(0).spec.map { case (k, v) => s"$k=$v" }.mkString(",")
            p0_time = formatter.format(new Date(ps(0).createTime))

            p1_name = ps(ps.size - 1).spec.map { case (k, v) => s"$k=$v" }.mkString(", ")
            p1_time = formatter.format(new Date(ps(ps.size - 1).createTime))
          }
        }
      }
      val lastUpdateTime = tblObj.properties.getOrElse("transient_lastDdlTime", "0").toLong
      val pipeline_name = tblObj.properties.getOrElse("pipeline_name", null)
      val source = tblObj.properties.getOrElse("source", null)

      val colStats = extractColStats(tblObj)
      tblObj.dataSchema.fields.map(f => {
        val isPartition = tblObj.partitionColumnNames.contains(f.name)
        if(colStats.contains(f.name)){
          val stat = colStats.get(f.name).get
          ColumnDef("", db, tbl, tblObj.tableType.name, tblObj.comment.getOrElse(""), locationStr,
            f.name, 0, f.dataType.simpleString, f.getComment().getOrElse(""),
            isPartition, "VIEW".equals(tblObj.tableType.name), tblObj.provider.getOrElse(""), lastUpdateTime,
            pipeline_name, source, p0_name, p0_time, p1_name, p1_time, stat._1, stat._2, stat._3, stat._4, stat._5, stat._6)
        }else{
          ColumnDef("", db, tbl, tblObj.tableType.name, tblObj.comment.getOrElse(""), locationStr,
            f.name, 0, f.dataType.simpleString, f.getComment().getOrElse(""),
            isPartition, "VIEW".equals(tblObj.tableType.name), tblObj.provider.getOrElse(""), lastUpdateTime,
            pipeline_name, source, p0_name, p0_time, p1_name, p1_time, null, null, null, null, null, null)
        }

      })
    } catch {
      case x: Exception => {
        println("============error retrieve table info: " + tbl)
        println(x)
        List(ColumnDef("", db, tbl, "", "", "", "", 0, "", "", false, false, "", 0, null, null, "", "", "", "", null, null, null, null, null, null))
      }
    }
  }).filter(c => !"".equals(c.colName))

}

def extractColStats(tblObj:CatalogTable)={
  val stats: CatalogStatistics = tblObj.stats.getOrElse(CatalogStatistics(0, Option.empty[BigInt]))
  val colStatMap = stats.colStats.map(st => {
    val colName = st._1
    val nullCount = if(!st._2.nullCount.isEmpty){
      st._2.nullCount.get.toString()
    }else {
      null
    }
    val distinctCount = if(!st._2.distinctCount.isEmpty){
      st._2.distinctCount.get.toString()
    }else {
      null
    }
    val max = if(!st._2.max.isEmpty){
      st._2.max.get.toString()
    }else {
      null
    }
    val min = if(!st._2.min.isEmpty){
      st._2.min.get.toString()
    }else{
      null
    }
    // only numberic columns have statistics, so they don't have avgLen or maxLen
    //    val avgLen = if(!st._2.avgLen.isEmpty){
    //      st._2.avgLen.get.toString
    //    }else{
    //      null
    //    }
    //    val maxLen = if(!st._2.maxLen.isEmpty){
    //      st._2.maxLen.get.toString
    //    }else{
    //      null
    //    }
    colName -> (nullCount, distinctCount, max, min, null, null)
  })
  colStatMap.toMap
}

// COMMAND ----------
val groupNum:Int = if(db_groups == null){
  4
}else{
  db_groups.toInt
}

val iter = loadTableDefs(spark, groupNum) // Iterator[(DataFrame, Int)]
iter.foreach(dfPair=>{
  val ep = export_file_path + "/" + dfPair._2
  dfPair._1.coalesce(1).write.mode("overwrite").option("header", true).csv(ep)
})


// COMMAND ----------

dbutils.fs.ls(export_file_path).zipWithIndex.foreach(pathPair => {
  dbutils.fs.ls(pathPair._1.path).foreach(subPath=>{
    if (subPath.path.endsWith(".csv")) {
      val copy_csv_file_path = s"${copy_csv_path}/${today}/${today}-${pathPair._2}.csv"
      dbutils.fs.cp(subPath.path, copy_csv_file_path)
    }
  })

})
