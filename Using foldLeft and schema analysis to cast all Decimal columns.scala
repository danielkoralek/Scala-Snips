// Using foldLeft to apply col.cast on every Decimal column
def castDecimals(df: DataFrame): DataFrame = {
  val cols = df.schema.fields.filter(f => f.dataType.toString.startsWith("Decimal")).map(_.name)
  cols.foldLeft(df) { (acc, c) => {
      acc.withColumn(c, col(c).cast("Decimal(38,9)"))
    }
  }
}
