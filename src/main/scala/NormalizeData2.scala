import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.hadoop.fs._



object NormalizeData2{

  val log: Logger = {
    Logger.getLogger(getClass.getName)
  }

  def main(argv: Array[String]): Unit = {
    val args = new NormalizeConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())

    val conf = new SparkConf().setAppName("Normalize Data")
    val sc = new SparkContext(conf)

    // get nlp-session if not created
    val spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    val input_path = args.input()
    val outputPath = args.output()

//    val input_path = "/Users/guanyingzhao/Desktop/cs651_project_data/stackoverflows_questions_no_answers.csv"
    //    val input_path = "/Users/guanyingzhao/Desktop/cs651_project_data/output_questionwithans_final.csv"
//    val outputPath = "/Users/guanyingzhao/Desktop/cs651_project_data/stackoverflows_outputs/"

    val outputDir = new Path(outputPath)
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    // read csv file
    val ques_df = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .load(input_path)
      .toDF()


    /**************** Data pre-processing ***************/
    val seq_ques_df = ques_df.select("id",  "title", "body", "creation_date", "answer_count", "favorite_count", "score", "tags", "reputation", "vote_type_id", "label")
      .filter(r => {
        // id, body, title, creation_date,
        !r.isNullAt(0) && !r.isNullAt(1) && !r.isNullAt(2) && !r.isNullAt(3) && !r.isNullAt(4) && !r.isNullAt(6) &&
          !r.isNullAt(7) && !r.isNullAt(8) && !r.isNullAt(9) && !r.isNullAt(10)
      })


    // filter null in favorite count col
    val seq_ques_rp = seq_ques_df.na.fill(0, Seq("favorite_count"))
    // stop word dict
    val stopWordDict = new StopWordsDict().STOP_WORDS_ARR
    // udf col int -> vector
    val int2vec = udf((value : Int) => Vectors.dense(value))
    // udf col string -> array
    val str2Arr = udf((value : String) => value.split("\\|"))


    val temp_df_stream = seq_ques_rp
      // replace <p> tags in body
      .withColumn("body_s", regexp_replace(col("body"), "<.*?>", ""))
      .drop("body")
      .withColumn("creation_date_v", int2vec(col("creation_date")))
      .drop("creation_date")
      .withColumn("answer_count_v", int2vec(col("answer_count")))
      .drop("answer_count")
      .withColumn("favorite_count_v", int2vec(col("favorite_count")))
      .drop("favorite_count")
      .withColumn("score_v", int2vec(col("score")))
      .drop("score")
      .withColumn("reputation_v", int2vec(col("reputation")))
      .drop("reputation")
      .withColumn("tags_a", str2Arr(col("tags")))
      .drop("tags")
      .withColumn("vote_type_id_a", str2Arr(col("vote_type_id")))
      .drop("vote_type_id")


      /**************** pipeline stages ****************/

      // title
      val regexTokenizer_tt = new RegexTokenizer()
        .setInputCol("title")
        .setOutputCol("title1")
        .setPattern("\\W")
      val remover_tt = new StopWordsRemover()
        .setInputCol("title1")
        .setOutputCol("filtered1")
        .setStopWords(stopWordDict)
      val cv_tt = new CountVectorizer()
        .setInputCol("filtered1")
        .setOutputCol("title_cv")
      val scaler_tt = new MinMaxScaler()
        .setInputCol("title_cv")
        .setOutputCol("title_f")

      // body
      val regexTokenizer_bd = new RegexTokenizer()
        .setInputCol("body_s")
        .setOutputCol("body1")
        .setPattern("\\W")
      val remover_bd = new StopWordsRemover()
        .setInputCol("body1")
        .setOutputCol("filtered2")
        .setStopWords(stopWordDict)
      val cv_bd = new CountVectorizer()
        .setInputCol("filtered2")
        .setOutputCol("body_cv")
      val scaler_bd = new MinMaxScaler()
        .setInputCol("body_cv")
        .setOutputCol("body_f")

      // creation date
      val scaler_cd = new MinMaxScaler()
        .setInputCol("creation_date_v")
        .setOutputCol("creation_date_f")
      // answer count
      val scaler_ac = new MinMaxScaler()
        .setInputCol("answer_count_v")
        .setOutputCol("answer_count_f")
      // favorite count
      val scaler_fc = new MinMaxScaler()
        .setInputCol("favorite_count_v")
        .setOutputCol("favorite_count_f")
      // score
      val scaler_sc = new MinMaxScaler()
        .setInputCol("score_v")
        .setOutputCol("score_f")
      // reputation
      val scaler_rp = new MinMaxScaler()
        .setInputCol("reputation_v")
        .setOutputCol("reputation_f")

      // tags
      val cv_tg = new CountVectorizer()
        .setInputCol("tags_a")
        .setOutputCol("tags_f")
      // vote_type_id
      val cv_vt = new CountVectorizer()
        .setInputCol("vote_type_id_a")
        .setOutputCol("vote_type_id_f")

      // concat vectors
      val assembler = new VectorAssembler()
        .setInputCols(Array("title_f", "body_f", "creation_date_f", "answer_count_f", "favorite_count_f", "score_f", "tags_f", "reputation_f", "vote_type_id_f"))
        .setOutputCol("features")

      // pipeline
      val pipeline = new Pipeline()
        .setStages(Array(
          // title
          regexTokenizer_tt,
          remover_tt,
          cv_tt,
          scaler_tt,
          // body
          regexTokenizer_bd,
          remover_bd,
          cv_bd,
          scaler_bd,
          // creation date, answer count, favorite count, score, reputation
          scaler_cd,
          scaler_ac,
          scaler_fc,
          scaler_sc,
          scaler_rp,
          // tags, vote_type_id
          cv_tg,
          cv_vt,
          // assembler
          assembler
        ))



      // udf col int, vector -> LabeledPoint
//      val iv2lbpt = udf((v1 : Int, v2: SparseVector) => LabeledPoint(v1, v2))
      val model = pipeline.fit(temp_df_stream)
      val trans_model = model.transform(temp_df_stream)
          .select("label", "features")


    import spark.implicits._


    val labeledPoints = trans_model
        .rdd
        .map(r => {
          val label = r.getInt(0)
          val features = r.getAs[SparseVector]("features")
          val lb = LabeledPoint(label, features)
          lb
        }).toDF()

    labeledPoints.write.format("libsvm").save(outputPath)

//      val labeledPoints = trans_model
//        .rdd
//        .map(r => {
//          val label = r.getInt(0)
//          val ml_features = r.getAs[SparseVector]("features")
//          val features = org.apache.spark.mllib.linalg.SparseVector.fromML(ml_features)
//          val lb = LabeledPoint(label, features)
//          lb
//        })
//
//
//      // save to lib svm format
//      MLUtils.saveAsLibSVMFile(labeledPoints, outputPath)

  }
}