package SqlToScala

import org.apache.spark.sql.SparkSession
import  org.apache.spark.sql.functions.when
import  org.apache.spark.sql.functions._
import  org.apache.spark.sql.expressions.Window



object CumulativeAdditionTransactions {
  def main(args: Array[String]): Unit = {
    val spark= SparkSession.builder().master("local").appName("Get the Balancing Amt after transactions").getOrCreate()

    val inpData=spark.read.format("csv").option("delimiter","|").option("header","true").option("inferSchema","true").load("/Users/z002gh2/naina/GITREPO/WorkspaceLearning/LearningScala/src/main/scala/SqlToScala/transactions.csv")


    val tData=inpData.withColumn("amt",
      when(col("direction") === "IN",col("amt"))
      .otherwise(col("amt")* -1)
    )

    val win_spec = Window.orderBy("id","amt").rowsBetween(Long.MinValue,0)

    val finlResult = tData.withColumn("balance",sum("amt").over(win_spec)).selectExpr("id","amt","balance")

    finlResult.show()
  }
}
