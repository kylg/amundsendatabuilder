// Databricks notebook source
dbutils.widgets.text("today", "", "today")
dbutils.widgets.text("export_path", "", "export_path")
dbutils.widgets.text("csv_path", "", "csv_path")
val today = dbutils.widgets.get("today")
val export_path = dbutils.widgets.get("export_path")
val copy_csv_path = dbutils.widgets.get("export_csv_path")
val export_file_path = s"${export_path}/${today}"
val copy_csv_file_path = s"${copy_csv_path}/${today}.csv"

// COMMAND ----------

// val databases = spark.sharedState.externalCatalog.listDatabases()
// spark.sharedState.externalCatalog.listTables("platform_test")

import org.apache.spark.sql.{DataFrame, SparkSession}

case class ColumnDef(tblId: String, dbName: String, tblName: String, tblType: String, tblDesc: String, tblLocation: String,
                     colName: String, colSortOrder: Int, colType: String, colDesc: String, isPartition: Boolean,
                     isView: Boolean, compressType: String, lastUpdateTime: Long, p0Name: String, p0Time: String, p1Name: String, p1Time: String)

def loadTableDefs(spark: SparkSession): DataFrame = {
  val dbs = spark.sharedState.externalCatalog.listDatabases()
  val columnDefList = dbs.flatMap(db => {
    spark.sharedState.externalCatalog.listTables(db).flatMap(tbl => {
      try {
        val tblObj = spark.sharedState.externalCatalog.getTable(db, tbl)
        println("============get table info for " + tbl)
        import java.net.URI
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
              import java.text.SimpleDateFormat
              val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
              p0_name = ps(0).spec.map { case (k, v) => s"$k=$v" }.mkString(",")
              import java.util.Date
              p0_time = formatter.format(new Date(ps(0).createTime))

              p1_name = ps(ps.size - 1).spec.map { case (k, v) => s"$k=$v" }.mkString(", ")
              p1_time = formatter.format(new Date(ps(ps.size - 1).createTime))
            }
          }
        }
        val lastUpdateTime = tblObj.properties.getOrElse("transient_lastDdlTime", "0").toLong
        tblObj.dataSchema.fields.map(f => {
          val isPartition = tblObj.partitionColumnNames.contains(f.name)
          ColumnDef("", db, tbl, tblObj.tableType.name, tblObj.comment.getOrElse(""), locationStr,
            f.name, 0, f.dataType.simpleString, f.getComment().getOrElse(""),
            isPartition, "VIEW".equals(tblObj.tableType.name), tblObj.provider.getOrElse(""), lastUpdateTime,
            p0_name, p0_time, p1_name, p1_time)
        })
      } catch {
        case x: Exception => {
          println("============error retrieve table info: " + tbl)
          println(x)
          List(ColumnDef("", db, tbl, "", "", "", "", 0, "", "", false, false, "", 0, "", "", "", ""))
        }
      }
    }).filter(c => !"".equals(c.colName))
  })
  spark.createDataFrame(columnDefList)
}

val df = loadTableDefs(spark)
df.coalesce(1).write.mode("overwrite").option("header", true).csv(export_file_path)



// COMMAND ----------

dbutils.fs.ls(export_file_path).foreach(p => {
  if (p.path.endsWith(".csv")) {
    dbutils.fs.cp(p.path, copy_csv_file_path)
  }
})
