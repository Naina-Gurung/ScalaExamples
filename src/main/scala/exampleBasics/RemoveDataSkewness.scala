package exampleBasics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, _}

object RemoveDataSkewness {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().appName("Remove data skewness").master("local").getOrCreate()

    val jsonPath = getClass.getResource("/SkewedData/Employee_03.json").getPath
    val df = spark.read.option("multiLine","true").json(jsonPath)

    df.show()

    println("Before Salting")
    val bartDf = df.repartition(2,col("id"))
    //bartDf.rdd.partitions.size

    import spark.sqlContext.implicits._

    bartDf
      .rdd
      .mapPartitionsWithIndex{case (i,rows) => Iterator((i,rows.size))}
      .toDF("partition_number","number_of_records")
      .show(false)


    println("After Salting")

    val slatedDF = df.withColumn("salt", substring(rand(),3,4).cast("bigint"))

    val BPart = slatedDF.repartition(2,col("salt"))

    BPart
      .rdd
      .mapPartitionsWithIndex{case (i,rows) => Iterator((i,rows.size))}
      .toDF("partition_number","number_of_records")
      .show(false)


  }

}
