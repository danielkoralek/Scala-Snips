miniNew.columns.map {
 col => (
   col,
   miniNew.select(col).except(miniOld.select(col)).count,
   miniOld.select(col).except(miniNew.select(col)).count
 )
}
.toSeq
.toDF("column", "diff_new", "diff_old")
.filter($"diff_new" + $"diff_old" > 0)
.d 

(.d is a hook for the display)
