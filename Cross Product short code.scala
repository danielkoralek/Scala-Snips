// Cartesian Product between  A and B

def crossProduct(a: Seq[Int], b: Seq[Int]): Seq[(Int, Int)] = for (x <- a; y <- b) yield (x, y)
val A: Seq[Int] = 1 to 3
val B: Seq[Int] = 4 to 6
crossProduct(A,B)
