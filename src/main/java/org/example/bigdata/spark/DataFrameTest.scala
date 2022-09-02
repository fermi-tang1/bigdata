package org.example.bigdata.spark

import org.apache.hadoop.conf.Configuration
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

/**
 * @Author: Fermi.Tang
 * @Date: Created in 15:18,2022/8/31
 */
object DataFrameTest {


  val schema_user = StructType {
    List(
      StructField("user_id", IntegerType, true),
      StructField("gender", StringType, true),
      StructField("age", IntegerType, true),
      StructField("other", StringType, true),
      StructField("code", StringType, true)
    )
  }

  val schema_rating = StructType {
    List(
      StructField("user_id", IntegerType, true),
      StructField("movie_id", IntegerType, true),
      StructField("avg", IntegerType, true),
      StructField("times", LongType, true),
    )
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local").appName("sql test").getOrCreate
    val userFilePath = "/Users/ftang/Downloads/user"
    val ratingFilePath = "/Users/ftang/Downloads/rating"
    val userDF = spark.read.schema(schema_user).csv(userFilePath)
    val ratingDF = spark.read.schema(schema_rating).csv(ratingFilePath)
    userDF.createOrReplaceTempView("user")
    ratingDF.createOrReplaceTempView("rating")
    // apply CombineFilter CollapseProject BooleanSimplification
    spark.sql("Select u.user_id, r.times from (select user_id, age from user where 1=1 and user_id < 500) u left join rating r on r.user_id = u.user_id and r.times > 1000 where u.age > 18 and r.times<10000").explain(true)

   // apply ConstantFolding PushDownPredicates ReplaceDistinctWithAggregate ReplaceExceptWithAntiJoin
    // FoldablePropagation 这个不知道什么意思
   spark.sql("Select distinct u.user_id, 1+2+3 as literal from user u left join rating r on r.user_id = u.user_id and r.avg > 2 where u.age > 18 except select u1.user_id, 1+2+3 as literal from user u1 where u1.gender = 'F'").explain(true)
  }

}
