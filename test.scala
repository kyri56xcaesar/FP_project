import org.apache.spark.sql.SparkSession
object test extends App
{

  val x = (1, 2, 3)
  val y = (1, 2, 4)

  val z = Seq((1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (4, 7), (6, 11), (2, 3))


  val j = z.filter(z => z._1 == 100).map({
    case (t, c) => c
    case _ => 124819

  })

  println(j)


}