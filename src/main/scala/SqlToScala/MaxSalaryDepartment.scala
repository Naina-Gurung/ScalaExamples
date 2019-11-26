package SqlToScala

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SparkSession}



object MaxSalaryDepartment {
  case class Dept(e_name : String,dept_name: String,month:String, Salary : Int)
  def main(args: Array[String]): Unit = {


    val sc=new SparkContext(new SparkConf().setAppName("Max Salary Department and month wise").setMaster("local"))
    val  spark = new SQLContext(sc)
    import spark.implicits._

    val file="/Users/z002gh2/naina/GITREPO/WorkspaceLearning/LearningScala/src/main/scala/SqlToScala/employee_salary.csv";
//    val emp = sc.textFile(file)
//      .mapPartitionsWithIndex( (idx, row) => if(idx==0) row.drop(1) else row ) //this is to drop the header
//      .map(x => (x.split("\\|")(0).toString, x.split("\\|")(1).toString,x.split("\\|")(2).toString,x.split("\\|")(3).toInt)).toDF("e_name","dept_name","month","salary")
//
//    val maxSal = emp.groupBy($"dept_name",$"month").max("salary").alias("max_salary")
//    maxSal.show()


    val rdd=sc.textFile(file)
    val header = rdd.first()
    val data=rdd.filter(row => row!=header)
    val empData=data.map(_.split("\\|")).map(x=>Dept(x(0).toString,x(1).toString,x(2).toString,x(3).toInt)).toDF()
    val max_sal=empData.groupBy($"dept_name",$"month").max("salary") as ("maximumSalary")
    max_sal.show()




  }

}
