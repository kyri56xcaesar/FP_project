import org.apache.spark.sql.SparkSession
object test extends App
{

  val x = Seq(1, 2, 3)

  val y = x.filter(c => c > 5)
  y.foreach(println)
  println(y)
  println(x)

}