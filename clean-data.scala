package dataproc.codelab
  
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.util.matching._
import java.util.Date
import org.apache.hadoop.fs._

object CleanData {
  def main(args: Array[String]) {
    if (args.length != 2) {
      throw new IllegalArgumentException(
          "Exactly 2 arguments are required: <inputPath> <outputPath>")
    }

    val inputPath = args(0)
    val outputPath = args(1)
    val outputDir = new Path(outputPath)
    val sc = new SparkContext(new SparkConf().setAppName("Clean Data"))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val conf = sc.hadoopConfiguration
    conf.set("google.cloud.auth.service.account.enable", "true")
    conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    val gcsFS = new Path("gs://stackoverflow_bigdata/").getFileSystem(conf)

    gcsFS.delete(outputDir, true)

    val df_questions = sqlContext.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load(inputPath+"*questions*")
    val df_posts = sqlContext.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load(inputPath+"stackoverflow*")
    val df_users = sqlContext.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load(inputPath+"users*")
    val df_votes = sqlContext.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load(inputPath+"votes*")

    val df_questions_distinct = df_questions.distinct()
    val df_posts_distinct = df_posts.distinct()
    val df_users_distinct = df_users.distinct()
    val df_votes_distinct = df_votes.distinct()

    val df_questions_correctType = df_questions_distinct.selectExpr("cast(id as int) id", 
                        "cast(title as String) title", 
                        "cast(body as String) body", 
                        "cast(creation_date as date) creation_date", 
                        "cast(answer_count as int) answer_count",
                        "cast(favorite_count as int) favorite_count",
                        "cast(score as int) score",
                        "cast(tags as int) tags",
                        "cast(view_count as int) view_count",
                        "cast(owner_user_id as int) owner_user_id")
    //df_questions_correctType.printSchema()
    //df_questions_correctType.show()

    val df_users_correctType = df_users_distinct.selectExpr("cast(id as int) id",
                        "cast(reputation as int) reputation")
    val df_votes_correctType = df_votes_distinct.selectExpr("cast(post_id as int) post_id",
                        "cast(vote_type_id as int) vote_type_id")

    val df_posts_join_users = df_posts_distinct.as("posts").join(df_users_distinct.as("users"), col("posts.owner_user_id") === col("users.id"), "inner")
      .select(col("posts.id"), col("posts.body"), col("posts.title"), col("posts.creation_date"), col("posts.answer_count"),col("posts.favorite_count"), col("posts.score"), col("posts.tags"), col("posts.view_count"), col("posts.owner_user_id"), col("users.reputation"))

//    val df_posts_join_users_votes = df_posts_join_users.as("postsUsers").join(df_votes.as("votes"), col("postsUsers.id") === col("votes.post_id"), "inner").drop("id", "creation_date", "post_id")
    val df_posts_join_users_votes = df_posts_join_users.as("postsUsers").join(df_votes_distinct.as("votes"), col("postsUsers.id") === col("votes.post_id"), "inner")
      .select(col("postsUsers.id"), col("postsUsers.title"), col("postsUsers.body"), col("postsUsers.creation_date"), col("postsUsers.answer_count"),col("postsUsers.favorite_count"), col("postsUsers.score"), col("postsUsers.tags"), col("postsUsers.view_count"), col("postsUsers.owner_user_id"), col("postsUsers.reputation"), col("votes.vote_type_id"))
      .distinct()

    val df_posts_merge_vote_type_id = df_posts_join_users_votes
      .groupBy("id")
      .agg(collect_list("vote_type_id").as("vote_type_id"))
      .withColumn("vote_type_id", arrayToStr(col("vote_type_id")))

    val df_posts_final = df_posts_join_users_votes.drop("vote_type_id").as("t1").join(df_posts_merge_vote_type_id.as("t2"), col("t1.id") === col("t2.id"), "inner")
      .select(col("t1.id"), col("t1.title"), col("t1.body"), col("t1.creation_date"), col("t1.answer_count"),col("t1.favorite_count"), col("t1.score"), col("t1.tags"), col("t1.view_count"), col("t1.owner_user_id"), col("t1.reputation"), col("t2.vote_type_id"))
      .distinct()
      .withColumn("label", lit(1).cast(IntegerType))
      .withColumn("creation_date", dateToUnixTimeStamp(col("creation_date")))

//    df_posts_join_users_votes.show()
    val df_questions_join_users = df_questions_correctType.as("questions").join(df_users_correctType.as("users"), col("questions.owner_user_id") === col("users.id"), "inner")
      .select(col("questions.id"), col("questions.title"), col("questions.body"), col("questions.creation_date"), col("questions.answer_count"),col("questions.favorite_count"), col("questions.score"), col("questions.tags"), col("questions.view_count"), col("questions.owner_user_id"), col("users.reputation"))
//    df_questions_join_users.show()

    val df_questions_join_users_votes = df_questions_join_users.as("questionsUsers").join(df_votes_correctType.as("votes"), col("questionsUsers.id") === col("votes.post_id"), "inner")
      .select(col("questionsUsers.id"), col("questionsUsers.title"), col("questionsUsers.body"), col("questionsUsers.creation_date"), col("questionsUsers.answer_count"),col("questionsUsers.favorite_count"), col("questionsUsers.score"), col("questionsUsers.tags"), col("questionsUsers.view_count"), col("questionsUsers.owner_user_id"), col("questionsUsers.reputation"), col("votes.vote_type_id"))
      .distinct()

    val df_questions_merge_vote_type_id = df_questions_join_users_votes
      .groupBy("id")
      .agg(collect_list("vote_type_id").as("vote_type_id"))
      .withColumn("vote_type_id", arrayToStr(col("vote_type_id")))

    val df_questions_final = df_questions_join_users_votes.drop("vote_type_id").as("t1").join(df_questions_merge_vote_type_id.as("t2"), col("t1.id") === col("t2.id"), "inner")
      .select(col("t1.id"), col("t1.title"), col("t1.body"), col("t1.creation_date"), col("t1.answer_count"),col("t1.favorite_count"), col("t1.score"), col("t1.tags"), col("t1.view_count"), col("t1.owner_user_id"), col("t1.reputation"), col("t2.vote_type_id"))
      .distinct()
      .withColumn("label", lit(0).cast(IntegerType))
      .withColumn("creation_date", dateToUnixTimeStamp(col("creation_date")))

//    df_questions_join_users_votes.select("*").where(df_questions_join_users_votes.col("id") === 56274647).show()

    df_questions_final.repartition(10).write.format("com.databricks.spark.csv").option("quote", "\"").option("escape", "\"").option("header", "true").save(outputPath+"question_final")
    df_posts_final.repartition(10).write.format("com.databricks.spark.csv").option("quote", "\"").option("escape", "\"").option("header", "true").save(outputPath+"questionwithans_final")
  }

  /*because I aggregated the vote_type_id column as a list of ids, on same rows, 
   to write to csv, need to convert to string using this functioa
  */
  val arrayToStr = udf((voteTypes: Seq[String]) => voteTypes match {
    case null => null
    case _ => s"""[${voteTypes.mkString("|")}]"""
  })

  val dateToUnixTimeStamp = udf((datetime: Date) => datetime match {
    case null => null
    case _ => {
      (datetime.getTime()/1000).toString()
    }
  })
}
