import org.apache.spark.sql.SparkSession


object Application1 extends App
{

// local configuration
  // spark session
    private val spark = SparkSession.builder.master("local[*]")
    .appName("Application Scenario 1: Reuters News Stories Analysis using Spark RDDs")
    .getOrCreate()
  private val hdfsPath = "hdfs://localhost:9000"
  //  FileSystem.setDefaultUri(spark.sparkContext.hadoopConfiguration, hdfsPath)


  // hdfs input/output paths
  private val application1_path = hdfsPath + "/reuters"

  private val categories_documents_path = application1_path + "/rcv1-v2.topics.qrels"
  private val documents_terms_path = application1_path + "/*.dat"
  private val terms_stem_path = application1_path + "/stem.termid.idf.map.txt"

  // Path to output file
  private val outputPath = hdfsPath + "/results/application1/"


  // configuration for the SoftNet cluster
//  val spark = SparkSession.builder
//    .appName("Project App1")
//    .master("yarn")
//    .config("spark.hadoop.fs.defaultFS", "hdfs://clu01.softnet.tuc.gr:8020")
//    //.config("spark.yarn.jars", "hdfs://clu01.softnet.tuc.gr:8020/user/xenia/jars/*.jar")
//    .config("spark.hadoop.yarn.resourcemanager.address", "http://clu01.softnet.tuc.gr:8189")
//    .config("spark.hadoop.yarn.application.classpath", "$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*,$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*")
//    //.enableHiveSupport()
//    .getOrCreate()

  // HDFS paths of files needed


//  val hdfsURI = "hdfs://localhost:9000"
//  FileSystem.setDefaultUri(spark.sparkContext.hadoopConfiguration, hdfsURI)
//  val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
//  private val application1_path = "/user/chrisa/Reuters"
//
//  private val categories_documents_path = application1_path + "/rcv1-v2.topics.qrels"
//  private val documents_terms_path = application1_path + "/*.dat"
//  private val terms_stem_path = application1_path + "/stem.termid.idf.map.txt"
//
//  // Path to output file
// private val outputPath = "/user/fp10/application1_results/"
//



  // Read all files and hold them to mem as RDDs (.textFile returns as RDD of Strings)
  // These files are UTF-8 encoded



  // prechecked for duplicates. ( there are none )
  //println(categories_documents.count())
  //println(categories_documents.distinct().count())

  // <category (C)> <document id (DOC)> 1
  private val categories_documents = spark.sparkContext.textFile(categories_documents_path)
    // split the context of each entry to words
    .map(entry => {
      // split the contents of each line via \\s+ superset delimiter for the space
      val split_words = entry.split("\\s+")
      // we dont care about the 1, keep only the category and the doc id
      // keep only the first two columns and hold them to a tuple
      // split_words(0): category, split_words(1): document id
      (split_words(0), split_words(1))
    })


  // Need to count how many documents each category has.
  private val doc_plurality_by_categories = categories_documents
    // attach count 1
    .map(cat_doc => (cat_doc._1, 1))
    // reduce by adding the ones... A.K.A. (sum)
    .reduceByKey(_+_)




  // file contains such  <document id (DOC)> [<term id (T)>]+
  // terms_documents will contain (term id, doc id)
  // We need to count how many documents belong to each term
  private val terms_documents = spark.sparkContext.textFile(documents_terms_path)
    // need to read each line that has  <did> and a "list" [<term id>:<weight>]+
    // since each line contains a list within we should flatten it after shaping it
    .flatMap(line => {
      // split each line into contents
      val split = line.split("\\s+")

      // split contains document_id with a list of terms:weights pairs
      // further split the terms weights
      val terms_weights = split.tail.map(_.split(":"))
      // keep only term id and doc id  for all (flattening this will result in only (termid, docid) pairs
      terms_weights.map(terms_weights => (terms_weights(0), split.head))

    })

  // how many documents each term has
  private val doc_plurality_by_terms = terms_documents
    // attach 1 for each occurrence
    .map(term => (term._1, 1))
    // attach each count (reducing like sum)
    .reduceByKey(_+_)



  // in order to calculate the intersection of doc_T and doc_C I need to find
  // the documents that  belong in a term and in a category at the same time.
  // GroupBy the documents. (document, ([categories]*, [terms]*))
  // if either categories or terms is None, remove the doc NOT NEEDED, pure join only joins on existence

  // need to join using document as a key, either reverse the tuples to set the doc as a key
  private val documents_grouped_by_cat = categories_documents
    // group each document by its categories
    .groupBy(_._2)
    // remap to (document, [(category, document)*]) to (document, [category*]), essentially remove the reduntant reference to the document
    .map(l => (l._1, l._2.map(t => t._1)))


  // This is an RDD which should hold ((category, term), (#documents))
  private val intersection_docs_with_cat_term  = terms_documents
    // group each document by its terms
    .groupBy(term_doc => term_doc._2)
    // map each entry of (document, [(term, document)*]) to (document, [term*]), essentially remove the redundant reference to the document
    .map(l => (l._1, l._2.map(t => t._1)))
    // join (with document as key)
    .join(documents_grouped_by_cat)
    // this holds (doc id as key, iterable terms and iterable categories)
    // flatmap so that pairs of (term id, category) are mapped with 1 so that they can be counted with reduce later
    // map from each entry the term to each other category and add the 1. Do this for all the terms of each entry.
    .flatMap(e => e._2._1.flatMap(t => e._2._2.map(c => ((t, c), 1))))
    // reduce by adding the ones for the matching pairs.
    .reduceByKey(_ + _)


  // reading the last file which will connect the term id to a stem
  // hold (term, stem)
  private val terms_stems = spark.sparkContext.textFile(terms_stem_path)
    .map(line => {
      val split = line.split("\\s+")

      (split(1), split(0))
    })



//  // Need to reconfigure without collect
//
//  private val collected_cats = doc_plurality_by_categories.collect()
//
//
//  private val term_category = doc_plurality_by_terms
//    .flatMap(term => collected_cats.map(c => (term._1, c._1)))
//

  // Need to create a pseudo "cartesian" product in order
  // to pair up all the categories with all the terms.

  // The categories are way less in amount
  // map "1" as key and group by it
  // In essence make the RDD horizontal not vertical...

  // doc_plurality_by_cats is (category, #documents)
  private val category_list = doc_plurality_by_categories
    // remap as 1 as key and category
    .map(c => (1, c._1 ))
    // group with 1 key, hold (1, Seq((1, category)*)
    .groupBy(_._1)
    // "essentially"make it horizontal
    // keep key 1 and a list of only the categories
    // (1, List(category*))
    .map(c => (1, c._2.map(x => x._2)))

  // Now apply the above horizontal categories with 1 as key to the terms so that they get paired
  private val term_category = doc_plurality_by_terms
    // remap with 1 as the key
    .map(t => (1, t._1))
    // join upon 1
    .join(category_list)
    // flatten each entry so that (term, category) is left
    .flatMap(e => e._2._2.map(x => (e._2._1, x)))




  // 47219 terms (total)
  // 37428 terms (only for pt3 file)
  // 103 categories
  // should be 3,855,084 pairs (only for pt3 file)
  // should be 4,863,557 pairs (total)
  //           4,863,557
  // indeed 3855084
  // intersected documents 173943 (only for pt3 file)


  // Calculations
  private val res_triads = term_category
    // join stems upon terms
    .join(terms_stems)
    // join the count of docs for each term
    .join(doc_plurality_by_terms)
    // must remap the key to category so I can join the doc pluralities of it
    .map({
      case (term, ((category, stem), dT_count)) =>
        (category, (stem, (term, dT_count)))
    })
    // now that category is key we can join its pluralities and remap
    .join(doc_plurality_by_categories)
    // now we must remap to find the count of intersected categories/terms, key should be (category, term)
    .map({
      case (category, ((stem, (term, dT_count)), dC_count)) =>
        ((term, category), (stem, dT_count, dC_count))

    })
    // can join with intersected.
    .join(intersection_docs_with_cat_term)
    .map({
      // match our entry's state
      case ((_, category),((stem, dT_count, dC_count), dC_dT_count)) =>

        // calculate the jaccard index, by part
        val denominator = dT_count + dC_count - dC_dT_count
        val jaccard_index = if (denominator == 0) 0.0 else dC_dT_count.toDouble / denominator.toDouble

        (category, stem, jaccard_index)
    })
    // remap to desired output format
    .map(e => s"<${e._1}>;<${e._2}>;<${e._3}>")

//  res_triads.take(10).foreach(println)


  // save to output path
  res_triads.saveAsTextFile(outputPath)


  // terminate spark session
  spark.stop()

}



//spark-submit --class <class name> --master local[8]
//<jar path>.jar