import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.rdd.RDD

object Application1 extends App
{
  private val spark = SparkSession.builder.master("local[*]")
    .appName("Application Scenario 1: Reuters News Stories Analysis using Spark RDDs")
    .getOrCreate()

  // HDFS paths of files needed
  private val hdfsPath = "hdfs://localhost:9000"
  FileSystem.setDefaultUri(spark.sparkContext.hadoopConfiguration, hdfsPath)

  private val application1_path = hdfsPath + "/reuters"

  private val categories_documents_path = application1_path + "/rcv1-v2.topics.qrels"
  private val documents_terms_path = application1_path + "/*.dat"
  private val terms_stem_path = application1_path + "/stem.termid.idf.map.txt"

  // Path to output file
  private val outputPath = hdfsPath + "/results/application1/"




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


  // Need to count in how many documents each category has.
  private val doc_plurality_by_categories = categories_documents
    // attach count 1
    .map(cat_doc => (cat_doc._1, 1))
    // reduce (sum)
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

      val terms_weights = split.tail.map(_.split(":"))
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
    .groupBy(cat_doc => cat_doc._2)
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
    .flatMap(e => e._2._1.flatMap(t => e._2._2.map(c => ((t, c), 1))))
    .reduceByKey(_ + _)






  // we need all <T, C> term category pairs
  private val collected_cats = doc_plurality_by_categories.collect()

  private val term_category = doc_plurality_by_terms
    .flatMap(term => collected_cats.map(c => (term._1, c._1)))


  // 37428 terms
  // 103 categories
  // should be 3855084 pairs
  // indeed 3855084
  // intersected documents 173943

  private val terms_stems = spark.sparkContext.textFile(terms_stem_path)
    .map(line => {
      val split = line.split("\\s+")

      (split(1), split(0))
    })

  val int_collected = intersection_docs_with_cat_term.collect().toMap
  val doc_C_collected = doc_plurality_by_categories.collect().toMap
  val doc_T_collected = doc_plurality_by_terms.collect().toMap


  private val res_triads = term_category
    .join(terms_stems)
    .map(t_c_s => {

      val jaccard_index = {
        // need to find the specific plurality intersection for given C and T
//        val intersection_count = intersection_docs_with_cat_term
//          .filter(t_c => (t_c._1._1, t_c._1._2) == (t_c_s._1, t_c_s._2._1))
//          .map({
//            case ((t, c), count) => count
//            case _ => 0
//          })
//          .fold(0)(_+_)
        val intersection_count = int_collected.get((t_c_s._1, t_c_s._2._1)).orElse(Option(0))
        val doc_C = doc_C_collected.get(t_c_s._2._1).orElse(Option(0))
        val doc_T = doc_T_collected.get(t_c_s._2._2).orElse(Option(0))
//
//        val doc_C = doc_plurality_by_categories
//          .filter(x => x._1 == t_c_s._2._1)
//          .map(x => x._2)
//          .fold(0)(_+_)

//        val doc_T = doc_plurality_by_terms
//          .filter(x => x._1 == t_c_s._1)
//          .map(x => x._2)
//          .fold(0)(_+_)

//        println(s"doc_C: ${doc_C} doc_T: ${doc_T} intr: ${intersection_count}")
        val denominator = doc_C.get + doc_T.get - intersection_count.get

        intersection_count.get / denominator.toDouble
      }

      (t_c_s._2._1, t_c_s._2._2, jaccard_index)
    })
    .map(e => s"<${e._1}>;<${e._2}>;<${e._3}>")

  //res_triads.collect().foreach(println)
  res_triads.saveAsTextFile(outputPath)

  spark.stop()

}