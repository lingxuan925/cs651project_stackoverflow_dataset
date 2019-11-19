package dataproc.codelab

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import scala.util.control._
import util.control.Breaks._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import org.apache.hadoop.fs._

object FormatTags{
  
  def main(args: Array[String]): Unit = {
    
    if (args.length != 3) {
      throw new IllegalArgumentException(
          "Exactly 2 arguments are required: <inputPath> <outputPath>")
    }

    val inputPath = args(0)
    val outputPath = args(1)
    val tempPath = args(2)
    val outputDir = new Path(outputPath)
    val sc = new SparkContext(new SparkConf().setAppName("Format Data"))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val conf = sc.hadoopConfiguration
    conf.set("google.cloud.auth.service.account.enable", "true")
    conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    val gcsFS = new Path("gs://stackoverflow_bigdata/").getFileSystem(conf)

    gcsFS.delete(outputDir, true)
    
    val spark = SparkSession.builder.getOrCreate()

    val tags_df = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .option("mode", "DROPMALFORMED")
    .load(inputPath+"*tags*")
    .toDF()

    val ques_df = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .option("mode", "DROPMALFORMED")
    .load(tempPath)
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
                  out += "|" + t_part
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
                    out = "|" + t_part + out
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
       .drop("id")
       .filter(col("split_tags") =!= "*****")
     
//     jdf.show()
     
     jdf.repartition(1).write.format("com.databricks.spark.csv")
      .option("header", "true").csv(outputPath)
  }
}
