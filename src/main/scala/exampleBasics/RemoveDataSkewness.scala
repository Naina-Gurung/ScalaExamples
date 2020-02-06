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
      .show(30)


    println("After Salting")

    val slatedDF = df.withColumn("salt", substring(rand(),3,4).cast("bigint"))

    val BPart = slatedDF.repartition(10,col("salt"))

    BPart
      .rdd
      .mapPartitionsWithIndex{case (i,rows) => Iterator((i,rows.size))}
      .toDF("partition_number","number_of_records")
      .show(100)

    //df:
    //+--------------------+---+------+-----+----+------+
    //|             address|age|deptid|ename|  id|salary|
    //+--------------------+---+------+-----+----+------+
    //|[Bangalore,Karnat...| 40|    11|SMITH|2222| 12000|
    //|  [HYDERABAD,Andhra]| 23|    21| FANG|2222| 30000|
    //|  [HYDERABAD,Andhra]| 23|    21| FANG|1111| 30000|
    //|  [HYDERABAD,Andhra]| 23|    21| FANG|1111| 30000|
    //|  [HYDERABAD,Andhra]| 23|    21| FANG|1111| 30000|
    //|  [HYDERABAD,Andhra]| 23|    21| FANG|1111| 30000|
    //|  [HYDERABAD,Andhra]| 23|    21| FANG|1111| 30000|
    //|  [HYDERABAD,Andhra]| 23|    21| FANG|1111| 30000|
    //|  [HYDERABAD,Andhra]| 23|    21| FANG|1111| 30000|
    //|  [HYDERABAD,Andhra]| 23|    21| FANG|1111| 30000|
    //|  [HYDERABAD,Andhra]| 23|    21| FANG|1111| 30000|
    //|  [HYDERABAD,Andhra]| 23|    21| FANG|1111| 30000|
    //|  [HYDERABAD,Andhra]| 23|    21| FANG|1111| 30000|
    //|  [HYDERABAD,Andhra]| 23|    21| FANG|1111| 30000|
    //|  [HYDERABAD,Andhra]| 23|    21| FANG|1111| 30000|
    //|  [HYDERABAD,Andhra]| 23|    21| FANG|1111| 30000|
    //|  [HYDERABAD,Andhra]| 23|    21| FANG|1111| 30000|
    //|  [HYDERABAD,Andhra]| 23|    21| FANG|1111| 30000|
    //|  [HYDERABAD,Andhra]| 23|    21| FANG|1111| 30000|
    //|  [HYDERABAD,Andhra]| 23|    21| FANG|1111| 30000|
    //+--------------------+---+------+-----+----+------+
    //only showing top 20 rows

    //Before Salting
    //+----------------+-----------------+
    //|partition_number|number_of_records|
    //+----------------+-----------------+
    //|               0|                2|
    //|               1|               84|
    //+----------------+-----------------+

    //After Salting
    //+----------------+-----------------+
    //|partition_number|number_of_records|
    //+----------------+-----------------+
    //|               0|                9|
    //|               1|                8|
    //|               2|               10|
    //|               3|                4|
    //|               4|               11|
    //|               5|                9|
    //|               6|               12|
    //|               7|                7|
    //|               8|                9|
    //|               9|                7|
    //+----------------+-----------------+

  }

}
