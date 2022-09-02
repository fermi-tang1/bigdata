package org.example.bigdata.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * @Author: Fermi.Tang
 * @Date: Created in 17:34,2022/8/19
 */
object SparkDistcp {
  def main(args: Array[String]): Unit = {
    // set user to root to access hdfs

    System.setProperty("HADOOP_USER_NAME", "root")

    // local environment variables
    //   var inputFilePath: String = "/user/root/input"
    //  var outputFilePath: String = "output5"
    //        var maxConcurrence: String = "5"
    //        var ignoreFailure: Boolean = false
    //        val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")

    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)

    var inputFilePath: String = ""
    var outputFilePath: String = ""
    var maxConcurrence: String = "5"
    var ignoreFailure: Boolean = false

    def initParams(args: List[String]): Unit = args match {
      case "-ifp" :: value :: tail =>
        inputFilePath = value
        initParams(tail)
      case "-ofp" :: value :: tail =>
        outputFilePath = value
        initParams(tail)
      case "-i" :: value :: tail =>
        ignoreFailure = value.toBoolean
        initParams(tail)
      case "-m" :: value :: tail =>
        maxConcurrence = value
        initParams(tail)
      case Nil =>
      case _ :: value :: tail => initParams(tail)
    }

    initParams(args.toList)
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("DEBUG")
    val filePaths = new ListBuffer[String]()

    val conf = new Configuration()

    // local hdfs
    conf.set("fs.defaultFS", "hdfs://localhost:9000")
    recursionDir(inputFilePath, filePaths, conf);
    filePaths.foreach(
      filePath =>
        try {
          sc.textFile(filePath, maxConcurrence.toInt).saveAsTextFile(filePath.replace("input", outputFilePath));
        } catch {
          case t: Throwable => t.printStackTrace()
            if (!ignoreFailure) {
              throw new Exception("failed to copy " + filePath)
            }
        })
  }

  def recursionDir(path: String, filePaths: ListBuffer[String], conf: Configuration) {
    val files = FileSystem.get(conf).listStatus(new Path(path))
    files.foreach { f => {
      if (!f.isDirectory) {
        filePaths += f.getPath.toString
      } else {
        recursionDir(f.getPath.toString, filePaths, conf)
      }
    }
    }
  }

}
