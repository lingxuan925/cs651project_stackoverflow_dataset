//import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.SparseVector
//import org.apache.spark.mllib.feature.Stemmer
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.mllib.util.MLUtils

import org.apache.spark.mllib.regression.LabeledPoint

object NormalizeData{
  def main(args: Array[String]): Unit = {
    // get nlp-session if not created
    val spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    // formatted question csv
    val path = "/Users/guanyingzhao/Desktop/cs651_project_data/stackoverflows_questions_no_answers.csv"
    val outputPath = "/Users/guanyingzhao/Desktop/cs651_project_data/stackoverflows_outputs/"
    // read csv file
    val ques_df = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .load(path)
      .toDF()
      .limit(1000)


    //    val struct2Arr = udf((s : String) => s.toSeq)
    //    val new_ques_df = ques_df.withColumn("new_title", struct2Arr(col("title")))
    val seq_ques_df = ques_df.select("id",  "title", "body", "creation_date", "answer_count", "favorite_count", "score", "tags", "reputation", "vote_type_id", "label").rdd
      .filter(r => {
        // id, body, title, creation_date,
        !r.isNullAt(0) && !r.isNullAt(1) && !r.isNullAt(2) && !r.isNullAt(3) && !r.isNullAt(4) && !r.isNullAt(6) &&
          !r.isNullAt(7) && !r.isNullAt(8) && !r.isNullAt(9) && !r.isNullAt(10)
      })
      .map(r => {
        val id = r.getInt(0)
        val title = r.getString(1)
        val body = r.getString(2).replaceAll("<.*?>", "")
        val creation_date = Vectors.dense(r.getInt(3))
        val answer_count = Vectors.dense(r.getInt(4))
        val favorite_count = Vectors.dense(if (r.isNullAt(5)) 0 else r.getInt(5))
        val score = Vectors.dense(r.getInt(6))
        val tags = r.getString(7).split("\\|")
        val reputations = Vectors.dense(r.getInt(8))
        val vote_type_id = r.getString(9).split("\\|")
        val label = r.getInt(10)
        (id, title, body, creation_date, answer_count, favorite_count, score, tags, reputations, vote_type_id, label)
      }).collect.toSeq

    val sentenceDataFrame = spark.createDataFrame(seq_ques_df).toDF("id", "title", "body", "creation_date", "answer_count", "favorite_count", "score", "tags", "reputation", "vote_type_id", "label")
    //    sentenceDataFrame.select("id", "title", "body", "creation_date", "answer_count", "favorite_count", "score", "tags", "reputation", "vote_type_id", "label").show()
    //    sentenceDataFrame.show()

    // stop word dict
    val stopWordDict = new StopWordsDict().STOP_WORDS_ARR

    /********* pipeline *********/
    val regexTokenizer_tt = new RegexTokenizer()
      .setInputCol("title")
      .setOutputCol("title1")
      .setPattern("\\W")
    val regexTokenizer_bd = new RegexTokenizer()
      .setInputCol("body")
      .setOutputCol("body1")
      .setPattern("\\W")

    val remover_tt = new StopWordsRemover()
      .setInputCol("title1")
      .setOutputCol("filtered1")
      .setStopWords(stopWordDict)
    val remover_bd = new StopWordsRemover()
      .setInputCol("body1")
      .setOutputCol("filtered2")
      .setStopWords(stopWordDict)

    val hashingTF_tt = new HashingTF()
      .setInputCol("filtered1")
      .setOutputCol("title_f")
    val hashingTF_bd = new HashingTF()
      .setInputCol("filtered2")
      .setOutputCol("body_f")

    val scaler_cd = new MinMaxScaler()
      .setInputCol("creation_date")
      .setOutputCol("creation_date_f")
    val scaler_ac = new MinMaxScaler()
      .setInputCol("answer_count")
      .setOutputCol("answer_count_f")
    val scaler_fc = new MinMaxScaler()
      .setInputCol("favorite_count")
      .setOutputCol("favorite_count_f")
    val scaler_sc = new MinMaxScaler()
      .setInputCol("score")
      .setOutputCol("score_f")
    val scaler_rp = new MinMaxScaler()
      .setInputCol("reputation")
      .setOutputCol("reputation_f")


    val cv_tg = new CountVectorizer()
      .setInputCol("tags")
      .setOutputCol("tags_f")
    val cv_vt = new CountVectorizer()
      .setInputCol("vote_type_id")
      .setOutputCol("vote_type_id_f")

    val assembler = new VectorAssembler()
      .setInputCols(Array("title_f", "body_f", "creation_date_f", "answer_count_f", "favorite_count_f", "score_f", "tags_f", "reputation_f", "vote_type_id_f"))
      .setOutputCol("features")

    val pipeline = new Pipeline()
      .setStages(Array(
        regexTokenizer_tt,
        regexTokenizer_bd,
        remover_tt,
        remover_bd,
        hashingTF_tt,
        hashingTF_bd,
        scaler_cd,
        scaler_ac,
        scaler_fc,
        scaler_sc,
        scaler_rp,
        cv_tg,
        cv_vt,
        assembler
      ))

    val model = pipeline.fit(sentenceDataFrame)
    val trans_model = model.transform(sentenceDataFrame)
      .select("label", "features")
    val labeledPoints = trans_model
      .rdd
      .map(r => {
        val label = r.getInt(0)
        val ml_features = r.getAs[SparseVector]("features")
        val features = org.apache.spark.mllib.linalg.SparseVector.fromML(ml_features)

        val pos = LabeledPoint(label, features)
        pos
      })

    // save to lib svm format
    MLUtils.saveAsLibSVMFile(labeledPoints, outputPath)

  }
}