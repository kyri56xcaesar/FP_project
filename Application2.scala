import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.FileSystem

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




  private val logs = spark.read.text(nmea_aegean_logs_path)



  // Q1: What is the number of tracked vessel positions per station per day?

  // Q2: What is the vessel id with the highest number of tracked positions?

  // Q3: What is the average SOG of vessels that appear in both station 8006 and station 10003 in the same day

  // Q4: What is the average Abs (Heading - COG) per station?

  // Q5: What are the Top-3 most frequent vessel statuses?

  logs.foreach(println)
}
