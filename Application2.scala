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




  private val logs = spark.read.option("header", "true").text(nmea_aegean_logs_path)



  // Q1: What is the number of tracked vessel positions per station per day?
  // I guess we should check for empty rows
  private val no_vessels_per_station = logs
    .filter(col("longitude").isNotNull && col("latitude").isNotNull && col("station").isNotNull && col("timestamp").isNotNull)
//    .groupBy(col("mmsi"))
//    .agg()
  no_vessels_per_station.take(40).foreach(println)

  println(logs.count())
  println(no_vessels_per_station.count())

  // Q2: What is the vessel id with the highest number of tracked positions?

  // Q3: What is the average SOG of vessels that appear in both station 8006 and station 10003 in the same day

  // Q4: What is the average Abs (Heading - COG) per station?

  // Q5: What are the Top-3 most frequent vessel statuses?
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

  logs.take(30).foreach(println)
}
