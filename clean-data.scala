package dataproc.codelab
  
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.util.matching._

object CleanData {
  def main(args: Array[String]) {
    if (args.length != 2) {
      throw new IllegalArgumentException(
          "Exactly 2 arguments are required: <inputPath> <outputPath>")
    }

    val inputPath = args(0)
    val outputPath = args(1)

    val sc = new SparkContext(new SparkConf().setAppName("Clean Data"))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val df_questions = sqlContext.read.format("csv").option("header", "true").load(inputPath+"*questions*")
    val df_posts = sqlContext.read.format("csv").option("header", "true").load(inputPath+"stackoverflow*")
    val df_users = sqlContext.read.format("csv").option("header", "true").load(inputPath+"users*")
    val df_votes = sqlContext.read.format("csv").option("header", "true").load(inputPath+"votes*")

    val df_questions_distinct = df_questions.distinct()
    val df_posts_distinct = df_posts.distinct()
    val df_users_distinct = df_users.distinct()
    val df_votes_distinct = df_votes.distinct()
/*
    val df_posts_join_users = df_posts_distinct.as("posts").join(df_users_distinct.as("users"), col("posts.owner_user_id") === col("users.id"), "inner")
      .select(col("posts.id"), col("posts.title"), col("posts.creation_date"), col("posts.answer_count"),col("posts.favorite_count"), col("posts.score"), col("posts.tags"), col("posts.view_count"), col("posts.owner_user_id"), col("users.reputation"))

//    val df_posts_join_users_votes = df_posts_join_users.as("postsUsers").join(df_votes.as("votes"), col("postsUsers.id") === col("votes.post_id"), "inner").drop("id", "creation_date", "post_id")
    val df_posts_join_users_votes = df_posts_join_users.as("postsUsers").join(df_votes_distinct.as("votes"), col("postsUsers.id") === col("votes.post_id"), "inner")
      .select(col("postsUsers.id"), col("postsUsers.title"), col("postsUsers.creation_date"), col("postsUsers.answer_count"),col("postsUsers.favorite_count"), col("postsUsers.score"), col("postsUsers.tags"), col("postsUsers.view_count"), col("postsUsers.owner_user_id"), col("postsUsers.reputation"), col("votes.vote_type_id"))
      .orderBy(desc("postsUsers.answer_count"))

    df_posts_join_users_votes.show()
*/
    val df_questions_join_users = df_questions_distinct.as("questions").join(df_users_distinct.as("users"), col("questions.owner_user_id") === col("users.id"), "inner")
      .select(col("questions.id"), col("questions.title"), col("questions.creation_date"), col("questions.answer_count"),col("questions.favorite_count"), col("questions.score"), col("questions.tags"), col("questions.view_count"), col("questions.owner_user_id"), col("users.reputation"))

    val df_questions_join_users_votes = df_questions_join_users.as("questionsUsers").join(df_votes_distinct.as("votes"), col("questionsUsers.id") === col("votes.post_id"), "inner")
      .select(col("questionsUsers.id"), col("questionsUsers.title"), col("questionsUsers.creation_date"), col("questionsUsers.answer_count"),col("questionsUsers.favorite_count"), col("questionsUsers.score"), col("questionsUsers.tags"), col("questionsUsers.view_count"), col("questionsUsers.owner_user_id"), col("questionsUsers.reputation"), col("votes.vote_type_id"))
//      .orderBy(desc("questionsUsers.answer_count"))

    val expr = "[0-9]".r
    val df_questions_join_users_votes_clean = df_questions_join_users_votes
//      .filter(!col("answer_count").rlike(expr))
      .filter(row => row.getAs[String]("answer_count").matches("""\d+"""))

    df_questions_join_users_votes_clean.show()
//    df_questions_join_users_votes.select("*").where(df_questions_join_users_votes.col("id") === 56274647).show()

  }
}
