package SqlToScala


import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object MinSalaryDeptWindowDF {

  def main(args: Array[String]): Unit = {

    val simpleData=Seq(("James","Sales",3000),
      ("Michael","Sales",4600),
      ("Robert","Sales",4100),
      ("Maria","Finance",3000),
      ("Raman","Finance",3000),
      ("Scott","Finance",3300),
      ("Jen","Finance",3900),
      ("Jeff","Marketing",3000),
      ("Kumar","Marketing",2000)
    )

   // val sc= new SparkContext(new SparkConf().setAppName("Salary per dept").setMaster("local"))
    val  spark = SparkSession.builder().appName("Min salary dept").master("local").getOrCreate()

    import spark.implicits._

    val df=simpleData.toDF("Name","Department","Salary")


    val w2= Window.partitionBy("Department").orderBy(col("Salary").asc)
    df.withColumn("row",row_number.over(w2)).where($"row" === 1).drop("row").show()

  }
}
/* input
+-------+----------+------+
|   Name|Department|Salary|
+-------+----------+------+
|  James|     Sales|  3000|
|Michael|     Sales|  4600|
| Robert|     Sales|  4100|
|  Maria|   Finance|  3000|
|  Raman|   Finance|  3000|
|  Scott|   Finance|  3300|
|    Jen|   Finance|  3900|
|   Jeff| Marketing|  3000|
|  Kumar| Marketing|  2000|
+-------+----------+------+

output:
+-----+----------+------+
| Name|Department|Salary|
+-----+----------+------+
|James|     Sales|  3000|
|Maria|   Finance|  3000|
|Kumar| Marketing|  2000|
+-----+----------+------+


 */
