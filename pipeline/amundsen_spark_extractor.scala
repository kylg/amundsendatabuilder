// Databricks notebook source
dbutils.widgets.text("today", "", "today")
dbutils.widgets.text("export_path", "", "export_path")
dbutils.widgets.text("csv_path", "", "csv_path")
val today = dbutils.widgets.get("today")
val export_path = dbutils.widgets.get("export_path")
val export_csv_path = dbutils.widgets.get("export_csv_path")

// COMMAND ----------

// val databases = spark.sharedState.externalCatalog.listDatabases()
// spark.sharedState.externalCatalog.listTables("platform_test")
import org.apache.spark.sql.{DataFrame, SparkSession}

case class ColumnDef(tblId:String, dbName:String, tblName:String, tblType:String, tblDesc:String, tblLocation:String, colName:String, colSortOrder:Int, colType:String, colDesc:String, isPartition:Boolean, isView:Boolean)

def loadTableDefs(sparkSession: SparkSession): DataFrame = {
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.{Row}
    val dbs = sparkSession.sharedState.externalCatalog.listDatabases().filter(_.equals("platform_test"))
    val list = List("string", "int", "long","byte","short","float","double","java.math.bigdecimal","array[byte]","boolean","java.sql.timestamp","java.sql.date","scala.collection.seq","scala.collection.map","org.apache.spark.sql.row")
    val columnDefList = dbs.flatMap(db => {
      sparkSession.sharedState.externalCatalog.listTables(db).flatMap(tbl => {
        var inPartition = false
        var inTblInfo = false
        val columnList = collection.mutable.MutableList[Row]()
        val partitionCols = collection.mutable.HashSet[String]()
        val dict = collection.mutable.Map[String, String]()
        sparkSession.sql("DESCRIBE formatted platform_test.test_dmga_daily_01").collect().foreach(row => {
          // some header row
          if("# partition information".equalsIgnoreCase(row.getString(0)) || "# col_name".equalsIgnoreCase(row.getString(0))){
            inPartition = true
          }else if("# Detailed Table Information".equalsIgnoreCase(row.getString(0))){
            inTblInfo = true
          }else{
            if(inTblInfo){
              dict.put(row.getString(0), row.getString(1))
            }else{
              if(inPartition){
                partitionCols.+=(row.getString(0))
              }else{
                columnList.+=(row)
              }
            }
          }
        })
        columnList.map(row => {
          ColumnDef("", db, tbl, dict.get("Type").getOrElse(""), "", dict.get("Location").getOrElse(""), row.getString(0), 0, row.getString(1), row.getString(2), partitionCols.contains(row.getString(0)), false)
        })

      })
    })
    sparkSession.createDataFrame(columnDefList)
  }

val df = loadTableDefs(spark)
df.coalesce(1).write.mode("overwrite").option("header", true).csv(export_path)



// COMMAND ----------

dbutils.fs.ls(export_path).foreach(p => {
  if(p.path.endsWith(".csv")){
    dbutils.fs.cp(p.path, export_csv_path)
  }
})
