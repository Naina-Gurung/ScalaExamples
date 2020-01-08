package exampleBasics

import org.apache.log4j.{Logger,Level}
import org.apache.spark.sql.SparkSession


object WriteToNestedJson {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder().master("local").appName("Write to Nested Json").getOrCreate()

    val data=spark.read.option("header","true").csv("/Users/z002gh2/naina/LEARNINGS/ScalaExamples/src/main/resources/writeToJson.csv")

    data.createOrReplaceTempView("employee")

    spark.sql("select company, department, collect_list(name) as department_employee from employee group by company, department ").createOrReplaceTempView("EmployeeDept")


    spark.sql("select company,collect_list(struct(department,department_employee))  as deptInfo from EmployeeDept group by company  ").toJSON.show(false)
  }

}
