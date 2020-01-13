//import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.SparseVector
import org.rogach.scallop._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
//import org.apache.spark.mllib.feature.Stemmer
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.mllib.util.MLUtils
import org.apache.log4j._
import org.apache.hadoop.fs._

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.ml.evaluation.RegressionEvaluator

object train{

  val log: Logger = {
    Logger.getLogger(getClass.getName)
  }

  def main(argv: Array[String]): Unit = {
    val args = new NormalizeConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())

    val conf = new SparkConf().setAppName("Compute Pairs PMI")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    // get nlp-session if not created
    val spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    val input_path = args.input()
    val outputPath = args.output()

    // Load training data
    val data = spark.read.format("libsvm").load(input_path)

    val splits = data.randomSplit(Array(0.9, 0.1), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    val lr = new LogisticRegression()
    .setMaxIter(1000)
    .setRegParam(0.01)
    .setElasticNetParam(0)

    // Fit the model
    val lrModel = lr.fit(training)

    // Print the coefficients and intercept for logistic regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

   val trainingSummary = lrModel.binarySummary

// Obtain the objective per iteration.

    // save to lib svm format
  //  MLUtils.saveAsLibSVMFile(labeledPoints, outputPath)
// Obtain the objective per iteration
   val objectiveHistory = trainingSummary.objectiveHistory
   println("objectiveHistory:")
   objectiveHistory.foreach(println)

  // for multiclass, we can inspect metrics on a per-label basis
   println("False positive rate by label:")
   trainingSummary.falsePositiveRateByLabel.zipWithIndex.foreach { case (rate, label) =>
   println(s"label $label: $rate")
  }

   println("True positive rate by label:")
   trainingSummary.truePositiveRateByLabel.zipWithIndex.foreach { case (rate, label) =>
  println(s"label $label: $rate")
   }

   println("Precision by label:")
   trainingSummary.precisionByLabel.zipWithIndex.foreach { case (prec, label) =>
  println(s"label $label: $prec")
}

   println("Recall by label:")
  trainingSummary.recallByLabel.zipWithIndex.foreach { case (rec, label) =>
  println(s"label $label: $rec")
}


   println("F-measure by label:")
   trainingSummary.fMeasureByLabel.zipWithIndex.foreach { case (f, label) =>
  println(s"label $label: $f")
}

   val accuracy = trainingSummary.accuracy
   val falsePositiveRate = trainingSummary.weightedFalsePositiveRate
   val truePositiveRate = trainingSummary.weightedTruePositiveRate
   val fMeasure = trainingSummary.weightedFMeasure
   val precision = trainingSummary.weightedPrecision
   val recall = trainingSummary.weightedRecall
   println(s"Accuracy: $accuracy\nFPR: $falsePositiveRate\nTPR: $truePositiveRate\n" +
  s"F-measure: $fMeasure\nPrecision: $precision\nRecall: $recall")

   // Compute raw scores on the test set.
   val prediction = lrModel.transform(test)
   val regEval = new RegressionEvaluator()
  .setPredictionCol("prediction")
  .setMetricName("mae")
   println("the mean absolute error is  "+ regEval.evaluate(prediction))
  }
}
