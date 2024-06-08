import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.functions._

import scala.math.Ordering.Float


object Application2 extends App {
  private val spark = SparkSession.builder.master("local[*]")
    .appName("Application Scenario 2: Vessel Trajectory Analytics in the Aegean Sea")
    .getOrCreate()


  private val hdfspath = "hdfs://localhost:9000"
  FileSystem.setDefaultUri(spark.sparkContext.hadoopConfiguration, hdfspath)

  private val application2_path = hdfspath + "/nmea_aegean"
  private val nmea_aegean_logs_path = application2_path + "/nmea_aegean.logs"

  private val Q1_outputPath = hdfspath + "/results/application2/Q1"
  private val Q2_outputPath = hdfspath + "/results/application2/Q2"
  private val Q3_outputPath = hdfspath + "/results/application2/Q3"
  private val Q4_outputPath = hdfspath + "/results/application2/Q4"
  private val Q5_outputPath = hdfspath + "/results/application2/Q5"


  private val logs = spark.read.option("header", "true").csv(nmea_aegean_logs_path)
  //logs.printSchema()
  //logs.show(10)

  // Q1: What is the number of tracked vessel positions per station per day?




  private val no_vessels_per_day_per_station = logs
    // Filtering for null data
    .filter(col("mmsi").isNotNull)
    .filter(col("station").isNotNull && col("timestamp").isNotNull)
    //Concating longtitude and latitude to create collumnt "tracked_position"
    .withColumn("tracked_position", concat_ws(",", col("longitude"), col("latitude")))
    //Dropping the unnecessary columns
    .drop("longitude", "latitude")
    //Since we need a per day per station result we groupBy accordingly
    .groupBy(col("station"), to_date(col("timestamp")))
    //We search for unique vessel positions
    .agg(
      countDistinct("tracked_position").as("unique_tracked_pos")
    )
    .withColumnRenamed("count", "no_vessels_per_day_station")
    .withColumnRenamed("to_date(timestamp)", "date")

  //  no_vessels_per_day_per_station
  //    .write
  //    .mode(SaveMode.Overwrite)
  //    .format("csv")
  //    .save(Q1_outputPath)


  //no_vessels_per_day_per_station.show(10)

  // no junk values
  //println(logs.count())
  //println(logs.filter((col("station").isNotNull && col("station").isNotNull)).count())


  // Q2: What is the vessel id with the highest number of tracked positions?
  // should count vessels with highest amount of unique longitude, latitude pairs
  // "tracked positions" should indicate unique pairs? (long, lat)
  private val highest_tracked_vessel = logs
    .filter(col("mmsi").isNotNull)
    .filter(col("longitude").isNotNull && col("latitude").isNotNull)
    //We add collumn track_position and concat longtitude and latitude same as Q1
    .withColumn("tracked_position", concat_ws(",", col("longitude"), col("latitude")))
    .drop("longitude", "latitude")
    //We groupBy Vessel ID since that is the what we are looking for
    .groupBy("mmsi")
    //We get the count of distinct positions and order them by descending order to find the highest
    .agg(
      countDistinct("tracked_position").as("no_tracked_positions")
    )
    .orderBy(desc("no_tracked_positions"))
    //we use limit(1) to keep only the first result
    .limit(1)

  //  highest_tracked_vessel
  //    .write
  //    .mode(SaveMode.Overwrite)
  //    .format("csv")
  //    .save(Q2_outputPath)

  //highest_tracked_vessel.show()

  // no junk values
  // println(logs.count())
  // println(logs.filter(col("mmsi").isNotNull).count())
  // 4000293
  // 4000293


  // Q3: What is the average SOG of
  //                  vessels that appear in both station 8006 and station 10003
  //                                                             in the same day
  private val station1 = "8006"
  private val station2 = "10003"

  private val unique_stations_per_day_per_vessel_amount = logs

    .filter(col("timestamp").isNotNull && col("station").isNotNull)
    .filter(col("speedoverground").isNotNull)
    //Keeping only the entries that contain station1 and station2
    .filter(col("station") === station1 || col("station") === station2)
    //grouping by Vessel ID and Date
    .groupBy(col("mmsi"), to_date(col("timestamp")).as("date"))
    //collecting the set of (stations) per day and vessel, NOTE: sets contain only unique items, so each station will be represented only once.
    .agg(
      size(collect_set(col("station"))).as("unique_stations_per_day_per_vessel_amount")
    )
    .orderBy(desc("unique_stations_per_day_per_vessel_amount"))
    //All the sets that have a size of 2 entries mean that these are vessels that appeared in both stations the same day
    .filter(col("unique_stations_per_day_per_vessel_amount") === 2)

  //  unique_stations_per_day_per_vessel_length.show()

  //Calculating avg_SOG
  private val avg_SOG_vessels_inStation8006and10003atSameDay = logs
    // right join the main data set with the result of the previous processing
    .join(unique_stations_per_day_per_vessel_amount, "mmsi")
    //for the vessels that appeared in both stations the same day
    .groupBy("mmsi")
    //calculating the avg_SOG
    .agg(
      avg(col("speedoverground").cast("float")).as(s"avg_SOG_per_vessel_in${station1}and${station2}InSameDay")
    )
  //avg_SOG_vessels_inStation8006and10003atSameDay.show()

  //  avg_SOG_vessels_inStation8006and10003atSameDay
  //    .write
  //    .mode(SaveMode.Overwrite)
  //    .format("csv")
  //    .save(Q3_outputPath)


  // Q4: What is the average Abs (Heading - COG) per station?
  private val avg_heading_minus_cog_per_station = logs
    .filter(col("station").isNotNull)
    .groupBy("station")
    .agg(
      avg(abs(col("heading").cast("float") - col("courseoverground").cast("float"))).as("avg_deviationOfVessels_per_station")
    )

  avg_heading_minus_cog_per_station.show()
  //  avg_heading_minus_cog_per_station
  //    .write
  //    .mode(SaveMode.Overwrite)
  //    .format("csv")
  //    .save(Q4_outputPath)


  // Q5: What are the Top-3 most frequent vessel statuses?
  private val top3_most_frequent_statuses = logs
    .filter(col("mmsi").isNotNull && col("status").isNotNull)
    .groupBy("status")
    .count()
    .withColumnRenamed("count", "vessel_amount")
    .orderBy(desc("vessel_amount"))
    //keeping the first 3 results
    .limit(3)

  //  top3_most_frequent_statuses
  //    .write
  //    .mode(SaveMode.Overwrite)
  //    .format("csv")
  //    .save(Q5_outputPath)

  //top3_most_frequent_statuses.show()

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