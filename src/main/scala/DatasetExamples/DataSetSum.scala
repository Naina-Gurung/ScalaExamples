package DatasetExamples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import  org.apache.spark.sql.functions._


object DataSetSum {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("org").setLevel(Level.OFF)

  case class BckgrndTest(userid:String,color: String,count:Integer)

  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder().appName("Testing DataSet").master("local").getOrCreate()
    val customSchema= StructType(Seq(StructField("userid",StringType,true),StructField("color",StringType,true),StructField("count",IntegerType,true)))


    import spark.implicits._
    val result= spark.read.schema(customSchema).option("delimiter",",").option("header",false).csv("/Users/z002gh2/naina/LEARNINGS/ScalaExamples/src/main/resources/event.csv").as[BckgrndTest]



    val finalResult= result.groupBy("color").agg(expr = sum("count") as "cnt")


finalResult.show()
    spark.stop()

  }

}
