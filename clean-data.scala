package dataproc.codelab
  
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


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
    val df_answers = sqlContext.read.format("csv").option("header", "true").load(inputPath+"*answers*")
    val df_users = sqlContext.read.format("csv").option("header", "true").load(inputPath+"users*")

    val result = df_questions.as("dfq").join(df_answers.as("dfa"), Seq("owner_user_id"), "left").select("dfq")

    result.show()

    //df_questions.show()
    //df_users.show()

    //val dfdrop = df.drop("id").drop("last_access_date").drop("profile_image_url").drop("website_url")
    //dfdrop.show()
  }
}
