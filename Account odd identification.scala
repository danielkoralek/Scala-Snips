// Scala code for identifying odds in transactional IN/OUT accounts.
// A few aggregation techniques being used.

val aggCols = tb.columns.filter(c => (c.startsWith("in_") || c.startsWith("out_"))).map(
 col => sum(col).as(col)
)

val summed = tb
   .where($"post_date".between("2019-08-01", "2019-08-31"))
   .groupBy($"savings_account__id")
   .agg(
     aggCols.head, aggCols.tail: _*
   )

display(
 summed
   .withColumn("profile", summed.columns.filter(c => (c.startsWith("in_") || c.startsWith("out_"))).map(summed.col _).foldLeft(lit(0)) {
     (a,b) => a + when(b > 0, 1).otherwise(0)
   }) 
   .select($"savings_account__id", $"profile")
   .sort($"profile".desc)
   .limit(100) 
)
