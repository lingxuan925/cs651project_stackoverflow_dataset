import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import scala.util.control._
import util.control.Breaks._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

object FormatTags{
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("Formate Data")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//    import sqlContext.implicits._
    
    val spark = SparkSession.builder.getOrCreate()
    val tags_path = "/Users/guanyingzhao/Desktop/cs651_project_data/stackoverflows_tags.csv"
    val ques_path = "/Users/guanyingzhao/Desktop/cs651_project_data/stackoverflows_questions.csv"
    val out_path = "/Users/guanyingzhao/Desktop/cs651_project_data/stackoverflows_split_tags.csv"
    // read csv file
    val tags_df = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .option("mode", "DROPMALFORMED")
    .load(tags_path)
    .toDF()

    val ques_df = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .option("mode", "DROPMALFORMED")
    .load(ques_path)
    .toDF()
//    .limit(1000)
    
    val tagsList = tags_df.select("tag_name").rdd.map(t => (t(0).toString())).collect().sortBy(-_.size)

    val tagsCol = ques_df
    .select("id","tags").rdd.map(r => {
      //println("=================="+r(0).toString())
      if (r.anyNull) {
        Row(r(0), "*****")
      } else {
        val srow = r(1).toString()
        val slen = srow.size
        var len = slen
        var p1 = true
        var p2 = false
        var i = 0
        var out = ""
        while (len > 0) {
          breakable{
            for (t <- tagsList) {
              val t_len = t.size
              if (t_len <= len) {
                val t_part = srow.substring(i, i + t_len)
                if (t_part == t) {
                  out += "," + t_part
                  i += t_len
                  len -= t_len
                  p1 = false
                  break
                }
              }
            }
          }
          
          // problem
          if (p1) {
            len = -1
            // set problem 2
            p2 = true
          } else {
            // reset problem 1
            p1 = true
          }
        }
        
        // do problem in reverse
        if (p2) {
          // reset all variables
          len = slen
          i = slen
          out = ""
          
          while (len > 0) {
            breakable{
              for (t <- tagsList) {
                val t_len = t.size
                if (t_len <= i) {
                  val t_part = srow.substring(i - t_len, i)
                  if (t_part == t) {
                    out = "," + t_part + out
                    i -= t_len
                    len -= t_len
                    p2 = false
                    break
                  }
                }
              }
            }
            
            // problem
            if (p2) {
              len = -1
              out = ",*****"
            } else {
              // reset problem state
              p2 = true
            }
          }
        }
        
        // chop head
        Row(r(0), out.substring(1))
      }//end else
     })
     
     val schema = new StructType()
      .add(StructField("id2", IntegerType, true))
      .add(StructField("split_tags", StringType, true))
      
     val ndf = spark.createDataFrame(tagsCol, schema)
     val jdf = ndf.join(ques_df, ndf("id2") === ques_df("id"), "inner")
     
     jdf.show()
     
//     val out_df = fdf
//     .write.format("com.databricks.spark.csv")
//     .option("header", "true").csv(out_path)
  }
}