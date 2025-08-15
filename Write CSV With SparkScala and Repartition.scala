val completePath = s3BucketPath + "/complete" + nameSufix

base
  .select(complete_selectCols.map(col): _*)
  .repartition(1) 
  .write
  .format("com.databricks.spark.csv")
  .option("header", true)
  .mode("overwrite")
  .save(completePath) 
