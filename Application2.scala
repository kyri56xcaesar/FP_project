import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.functions._


object Application2 extends App
{
  private val spark = SparkSession.builder.master("local[*]")
    .appName(("Application Scenario 2: Vessel Trajectory Analytics in the Aegean Sea"))
    .getOrCreate()


  private val hdfspath = "hdfs://localhost:9000"
  FileSystem.setDefaultUri(spark.sparkContext.hadoopConfiguration, hdfspath)

  private val application2_path = hdfspath + "/nmea_aegean"
  private val nmea_aegean_logs_path = application2_path + "/nmea_aegean.logs"

  private val outputPath = hdfspath + "/results/application2"




  private val logs = spark.read.option("header", "true").csv(nmea_aegean_logs_path)
  logs.printSchema()
  logs.show(10)

  // Q1: What is the number of tracked vessel positions per station per day?
  // I guess we should check for empty rows

  private val no_vessels_per_day_per_station = logs
    .filter(col("mmsi").isNotNull)
    .filter((col("station").isNotNull && col("timestamp").isNotNull))
    .withColumn("tracked_position", concat_ws(",", col("longitude"), col("latitude")))
    .drop("longitude", "latitude")
    .groupBy(col("station"), to_date(col("timestamp")))
    .agg(
      countDistinct("tracked_position").as("unique_tracked_pos")
    )
    .withColumnRenamed("count", "no_vessels_per_day_station")
    .withColumnRenamed("to_date(timestamp)", "date")

  no_vessels_per_day_per_station.show(10)

  // no junk values
  //println(logs.count())
  //println(logs.filter((col("station").isNotNull && col("station").isNotNull)).count())

  /*
  */

  // Q2: What is the vessel id with the highest number of tracked positions?
  // should count vessels with highest amount of unique longitude, latitude pairs
  // "tracked positions" should indicate unique pairs? (long, lat)
  private val highest_tracked_vessel = logs
    .filter(col("mmsi").isNotNull)
    .filter(col("longitude").isNotNull && col("latitude").isNotNull)
    .withColumn("tracked_position", concat_ws(",", col("longitude"), col("latitude")))
    .drop("longitude", "latitude")
    .groupBy("mmsi")
    .agg(
      countDistinct("tracked_position").as("no_tracked_positions")
    )
    .orderBy(desc("no_tracked_positions"))
    .limit(1)

  highest_tracked_vessel.show()

  // no junk values
  // println(logs.count())
  // println(logs.filter(col("mmsi").isNotNull).count())
  // 4000293
  // 4000293



  // Q3: What is the average SOG of vessels that appear in both station 8006 and station 10003 in the same day
  private val avg_SOG_vessels_inStation8006and10003atSameDay = logs
    .filter(col("timestamp").isNotNull && col("station").isNotNull)
    .filter(col("speedoverground").isNotNull)
    //.groupBy(col("mmsi"), to_date(col("timestamp")))
    .agg(collect_set(col("station")).as("stations"))
    .filter(array_contains(col("stations"), "10003") && array_contains(col("stations"), "8006"))


  avg_SOG_vessels_inStation8006and10003atSameDay.show()



  // Q4: What is the average Abs (Heading - COG) per station?




  // Q5: What are the Top-3 most frequent vessel statuses?
  private val top3_most_frequent_statuses = logs
    .filter(col("mmsi").isNotNull && col("status").isNotNull)
    .groupBy("status")
    .count()
    .withColumnRenamed("count", "vessel_amount")
    .orderBy(desc("vessel_amount"))
    .limit(3)

  top3_most_frequent_statuses.show()

  // no junk values
  // println(logs.count())
  // println(logs.filter(col("mmsi").isNotNull && col("status").isNotNull).count())
  // 4000293
  // 4000293

  //
//  - timestamp: the timestamp of the acquired vessel position
//  - station: the id of the station which acquired the corresponding vessel position
//  - mmsi: maritime mobile service identity, consider this as the vessel Id
//  - longitude: geographic longitude of each timestamped vessel position
//  - latitude: geographic latitude of each timestamped vessel position
//  - Speed Over Ground (SOG): is the speed of the vessel relative to the surface of the earth
//  - Course Over Ground (COG): is the actual direction of progress of a vessel, between two
//  points, with respect to the surface of the earth
//  - Heading: is the compass direction in which the vessel bow is pointed
//  - Status:

//  o 0 = underway using engine
//  o 1 = at anchor
//  o 2 = not under command
//  o 3 = restricted maneuverability
//  o 4 = constrained by her draught
//  o 5 = moored
//  o 6 = aground
//  o 7 = engaged in fishing
//  o 8 = underway sailing
//  o 9 & 10 = reserved for future amendment
//  o 11 = power-driven vessel towing astern
//  o 12 = power-driven vessel pushing ahead or towing alongside
//  o 13 = reserved for future use
//  o 14 = AIS-SART Active (Search and Rescue Transmitter), AIS-MOB (Man Overboard), AISEPIRB (Emergency Position Indicating Radio Beacon):
//  o 15 = undefined = default (also used by AIS-SART, MOB-AIS, and EPIRB-AIS under test).

}
