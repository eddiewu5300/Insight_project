package spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.functions.udf
import scala.io.Source
import org.apache.spark.sql.util._
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

// spark-submit --class spark.text_processing --master spark://<master_ip>:7077  --executor-memory 5G --driver-memory 5G project-assembly-1.0.jar

object text_processing{

  def mapper(line:String): (String,Array[Float]) = {
      val fields = line.split(" ", 2);
      val word = fields(0);
      val vector = fields(1).split(" ").map(_.toFloat).toArray; 
      // val wv:WV = WV(word,vector) 
      return (word,vector)
    }

  def load_model(): scala.collection.Map[String,Array[Float]] = {
    val lines = sc.textFile("./scala/glove.6B.50d.txt")
    val wvs = lines.map(mapper)
    val dic = wvs.collectAsMap()
    return dic
    }

  def preproccess(reviews: DataFrame) :  DataFrame={

    val filter_text = (text: String) => {
      text.toString.replaceAll("""[,.!?:;&*%#@$^~'"{}:;|/><+=-]""", "").trim.toLowerCase;       
    }

    val filter = udf(filter_text)
    val filtered = reviews_clean.withColumn("filtered", filter(col("review_body")))
  
    val tokenizer = new Tokenizer().setInputCol("filtered").setOutputCol("tokenized")
    val tokenized = tokenizer.transform(filtered)
    
    val remover = new StopWordsRemover().setInputCol("tokenized").setOutputCol("removed")
    val removed = remover.transform(tokenized)  

    val tolemma = (text: Array[String]) => {
      val props = new Properties()
      props.put("annotators", "lemma")
      val pipeline = new StanfordCoreNLP(props)
      text.map(_.get(classOf[LemmaAnnotation]))
    }
  
    val lemmar = udf(tolemma)
    val lemma = removed.withColumn("lemma", lemmar(col("removed")))

    return lemma
  }

  def get_vec(ls: Array[String], dic: scala.collection.Map[String,Array[Float]]) : Array[Float]= {
    var embeded_review : Array[Float] = Array.fill(50)(0.00f);
  
    for (word <- ls) {
        val vector = scala.util.Try(dic(word)).getOrElse(Array.fill(50)(0.00f));
        embeded_review =  embeded_review.zip(vector).map { case (x, y) => x + y };
    }
    return embeded_review
  }

  def dotProduct(x: Array[Float], y: Array[Float]): Float = {
    (for((a, b) <- x zip y) yield a * b) sum
  }
  
  def magnitude(x: Array[Float]): Float = {
    math.sqrt(x map(i => i*i) sum).toFloat
  }

  def cosineSimilarity(x: Array[Float], y: Array[Float]): Float = {
    dotProduct(x, y)/(magnitude(x) * magnitude(y))
  }

  def calculate_sim(ls: List[Array[Float]]): Array[Float] = {
    val combine = ls.combinations(2).toArray
    var result = new scala.collection.mutable.ListBuffer[Float]()
    for (i <- combine){
      val sim = cosineSimilarity(i(0), i(1))
      result += sim
    }
    return result.toArray
  }

  def saveToCassandra(df: DataFrame, table: String) = {
    df.printSchema()
    df.write.format("org.apache.spark.sql.cassandra").
      options(Map("table" -> "reviews", "keyspace" -> "project")).
      mode(SaveMode.Append).
      save()
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val spark = SparkSession(sc).builder()
      .config("spark.cassandra.connection.host", Config.cassandraHost)
      .getOrCreate()

    val df = spark.read.parquet("s3a://amazondata/parquet/product_category=Apparel/part-00000-495c48e6-96d6-4650-aa65-3c36a3516ddd.c000.snappy.parquet")

    val reviews = df.select(col("customer_id"),col("review_body"))
    val reviews_clean = reviews.filter("review_body is not null")

    val dic = load_model()
    val text = preproccess(reviews_clean)

    val sen_vec = text.select(col("customer_id"),col("removed")).rdd
      .map(x=> (x(0).asInstanceOf[String], 
                x(1).asInstanceOf[scala.collection.mutable.WrappedArray[String]].toArray))
      .map(x=> (x._1, (List(get_vec(x._2,dic),1))))
            
    val reduce = sen_vec.reduceByKey(_:::_)
      .map(x=> (x._1.asInstanceOf[String], x._2.asInstanceOf[List[Array[Float]]]))

    val similarity = reduce.map(x => (x._1, x._2, calculate_sim(x._2)))
      
    val schema = new StructType()
        .add(StructField("user_id", String, True))
        .add(StructField("review", List[Array[Float]], True))
        .add(StructField("similarity", Array[Float], True))
                
    val result = spark.createDataFrame(similarity, schema)
    saveToCassandra(result)
  }
}
































