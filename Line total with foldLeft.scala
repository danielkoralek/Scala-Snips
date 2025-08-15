def generateLineTotal(colName: String)(movements: DataFrame): DataFrame = {
    val sumColumns = movements.columns.filter(x => (x != "savings_account__id")).map(movements.col _) 
    movements
     .withColumn(colName, sumColumns.foldLeft(lit(0)) { (a,b) => (a + b) })
}
