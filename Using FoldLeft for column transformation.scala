// Sorts the lists inside the record first to have matching hash for comparison.
def sortTheLists(excludeList: List[String])(df: DataFrame): DataFrame = {
 val cols = df.columns.filter(c => excludeList.contains(c))
 cols.foldLeft(df) { (acc, e) => {
     acc.withColumn(e, array_sort(col(e)))
   }
 }
}

// Define the function that creates the hash
def makeLineHash(df: DataFrame): DataFrame = {
 df
   .withColumn("linehash", hash(df.columns.map(col):_*))
   .select("customer__id","date","linehash")
}

val fixed_new_df = new_df.transform(sortTheLists(excludeList))//.transform(makeLineHash)
val fixed_old_df = old_df.transform(sortTheLists(excludeList))//.transform(makeLineHash)

val hashed_new_df = fixed_new_df.transform(makeLineHash)
val hashed_old_df = fixed_old_df.transform(makeLineHash)
