import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, hour, dayofweek, month, avg}
import org.apache.spark.sql.DataFrame
object NycTaxi extends App{
  // Create SparkSession
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("TripDataAnalysisDFs")
    .getOrCreate()

  val hdfsPath = "hdfs://localhost:9000/nyctaxi/"
  val df = spark.read.parquet(hdfsPath)


  // Calculate average trip distance per hour of the day
  val avgDistancePerHour = df.groupBy(hour(col("tpep_pickup_datetime"))
      .alias("pickup_hour"))
    .agg(avg("trip_distance").alias("avg_trip_distance"))
    .orderBy("pickup_hour")

  // Calculate total trip count per day of the week
  val tripCountPerDayOfWeek = df.groupBy(dayofweek(col("tpep_pickup_datetime"))
      .alias("pickup_dayofweek"))
    .count()
    .orderBy("pickup_dayofweek")

  // Calculate total trip count per month
  val tripCountPerMonth = df.groupBy(month(col("tpep_pickup_datetime"))
      .alias("pickup_month"))
    .count()
    .orderBy("pickup_month")

  // Show the results
  println("Average trip distance per hour of the day:")
  avgDistancePerHour.show()

  println("Total trip count per day of the week:")
  tripCountPerDayOfWeek.show()

  println("Total trip count per month:")
  tripCountPerMonth.show()

  // Stop SparkSession
  spark.stop()
}
