package org.example.bigdata.spark

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Fermi.Tang
 * @Date: Created in 16:16,2022/8/16
 */
object InvertedIndex {

  def main(args: Array[String]) = {
    val input = "source"
    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("DEBUG")
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val fileList = fs.listFiles(new Path(input), true)
    var totalRdd = sc.emptyRDD[(String, String)]
    while (fileList.hasNext) {
      val filePath = new Path(fileList.next().getPath.toString)
      val fileName = filePath.getName
      // parse file to (fileName, word)
      val rdd = sc.textFile(filePath.toString).flatMap(_.split(" ").map((fileName, _)))
      totalRdd = totalRdd.union(rdd)
    }
    // Map to ((fileName, Word), frequency)
    val rdd1 = totalRdd.map(word => {
      (word, 1)
    }).reduceByKey(_ + _)
    //6.Map ((fileName, Word), frequency) to (Word, (fileName, frequency))
    val cRdd1 = rdd1.map(word => {
      (word._1._2, String.format("(%s,%s)", word._1._1, word._2.toString))
    })
    val cRdd2 = cRdd1.reduceByKey(_ + "," + _)
    val cRdd3 = cRdd2.map(word => String.format("\"%s\",{%s}", word._1, word._2))
    val output = "source/output"
    cRdd3.repartition(1).saveAsTextFile(output)
  }
}
