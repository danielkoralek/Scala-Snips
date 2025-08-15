val countWindow = Window.partitionBy('loan__id).orderBy('date)

val df = spark.table("dataset.financeira_credit_operation_installment_snapshot")
 .filter('loan__id === "5c38da13-ec6a-41d2-9a28-f177adb34fba" && 'due_date.isNotNull)
 .select("loan__id", "date", "index")
 .groupBy('loan__id, 'date)
 .agg(count('index).as("open_installments"))
 .withColumn("open_count", lag('open_installments, 1, 0).over(countWindow))

df
 .filter('open_installments =!= 'open_count)
 .drop('open_count)
 .sort('loan__id, 'date)
 .d

// .d -> display(df)
