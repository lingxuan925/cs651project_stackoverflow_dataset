package dataproc.codelab
  
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.util.matching._
import org.apache.spark.sql.types._
import java.sql.Timestamp
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
                        "cast(creation_date as String) creation_date", 
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
/*
    val df_posts_join_users = df_posts_distinct.as("posts").join(df_users_distinct.as("users"), col("posts.owner_user_id") === col("users.id"), "inner")
      .select(col("posts.id"), col("posts.body"), col("posts.title"), col("posts.creation_date"), col("posts.answer_count"),col("posts.favorite_count"), col("posts.score"), col("posts.tags"), col("posts.view_count"), col("posts.owner_user_id"), col("users.reputation"))

//    val df_posts_join_users_votes = df_posts_join_users.as("postsUsers").join(df_votes.as("votes"), col("postsUsers.id") === col("votes.post_id"), "inner").drop("id", "creation_date", "post_id")
    val df_posts_join_users_votes = df_posts_join_users.as("postsUsers").join(df_votes_distinct.as("votes"), col("postsUsers.id") === col("votes.post_id"), "inner")
      .select(col("postsUsers.id"), col("postsUsers.title"), col("postsUsers.body"), col("postsUsers.creation_date"), col("postsUsers.answer_count"),col("postsUsers.favorite_count"), col("postsUsers.score"), col("postsUsers.tags"), col("postsUsers.view_count"), col("postsUsers.owner_user_id"), col("postsUsers.reputation"), col("votes.vote_type_id"))
      .orderBy("postsUsers.answer_count")
      .distinct()
      .withColumn("label", lit(1))
*/
//    df_posts_join_users_votes.show()
    val df_questions_join_users = df_questions_correctType.as("questions").join(df_users_correctType.as("users"), col("questions.owner_user_id") === col("users.id"), "inner")
      .select(col("questions.id"), col("questions.title"), col("questions.body"), col("questions.creation_date"), col("questions.answer_count"),col("questions.favorite_count"), col("questions.score"), col("questions.tags"), col("questions.view_count"), col("questions.owner_user_id"), col("users.reputation"))
//    df_questions_join_users.show()

    val df_questions_join_users_votes = df_questions_join_users.as("questionsUsers").join(df_votes_correctType.as("votes"), col("questionsUsers.id") === col("votes.post_id"), "inner")
      .select(col("questionsUsers.id"), col("questionsUsers.title"), col("questionsUsers.body"), col("questionsUsers.creation_date"), col("questionsUsers.answer_count"),col("questionsUsers.favorite_count"), col("questionsUsers.score"), col("questionsUsers.tags"), col("questionsUsers.view_count"), col("questionsUsers.owner_user_id"), col("questionsUsers.reputation"), col("votes.vote_type_id"))
      .orderBy("questionsUsers.answer_count")
      .distinct()
      .withColumn("label", lit(0).cast(IntegerType))

    val df_questions_join_users_votes_clean = df_questions_join_users_votes
      /*.filter(row => row match {
        case Row(id: Integer, title: String, body: String, creation_date: String, answer_count: Integer, favourite_count: Integer, score: Integer, tags: String, view_count: Integer, owner_user_id: Integer, reputation:  Integer, vote_type_id: Integer, label: Integer) => false 
        case _ => true
      })
*/
      .filter(row => {
        row.getAs("answer_count").isInstanceOf[String]
      })

//    df_questions_join_users_votes.select("*").where(df_questions_join_users_votes.col("id") === 56274647).show()

    df_questions_join_users_votes_clean.repartition(8).write.format("com.databricks.spark.csv").option("header", "true").save(outputPath+"question_new.csv")
//    df_posts_join_users_votes.repartition(8).write.format("com.databricks.spark.csv").option("header", "true").save(outputPath+"posts_new.csv")
  }
}
