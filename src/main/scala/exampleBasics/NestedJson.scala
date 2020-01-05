package exampleBasics

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object NestedJson {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local").appName("PArsing Nested JSon").getOrCreate()


    val empData=spark.read.json("/Users/z002gh2/naina/LEARNINGS/ScalaExamples/src/main/resources/Employee.json")

    val deptData=spark.read.json("/Users/z002gh2/naina/LEARNINGS/ScalaExamples/src/main/resources/Dept.json")


    val emp= empData
      .withColumn("ename",upper(col("ename")))
      .withColumn("city",upper(col("address.city")))
      .withColumn("state",upper(col("address.state")))
      .select("id","ename","age","salary","deptid","city","state")

    emp.show()

    println("-----------------------------------------------------")

    println(emp.rdd.getNumPartitions)

    println("-----------------------------------------------------")

    println(emp.repartition(4,col("state")).rdd.getNumPartitions)

    emp.write.csv("/Users/z002gh2/naina/LEARNINGS/ScalaExamples/src/main/resources/Employee_output")

    println("-----------------------------------------------------")


    val dept=deptData.select("deptid","deptname").toDF()
      .withColumn("deptname",upper(col("deptname")))

    val rslt=emp.join(dept,emp("deptid")===dept("deptid")).select(dept("deptid"),dept("deptname"),emp("id"))
      .groupBy("deptid","deptname").agg(count(emp("id")).alias("empcount"))

    println("logical plan")
    rslt.explain(true)
    rslt.createOrReplaceTempView("Department_count")

    spark.sql("select * from Department_count")

    spark.stop()
  }

}

/*
output:
+------+---------+--------+
|deptid| deptname|empcount|
+------+---------+--------+
|    11|       IT|       1|
|    31|MARKETING|       1|
|    41|  FINANCE|       1|
|    21|       HR|       1|
|    51|    ADMIN|       2|
+------+---------+--------+
 */