package exampleBasics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object PrintEvenColumns {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("org").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {
    val spark=  SparkSession.builder().master("local").getOrCreate()

    val path=getClass.getResource("/EmployeeDetails.json").getPath

    val empDf=spark.read.json(path)

    val parentColumns= empDf.columns.toList

    print(parentColumns)
    val evenCols= parentColumns.zipWithIndex.filter(_._2 % 2 == 0).map(_._1).toSeq

    val childDf= empDf.select(evenCols.head, evenCols.tail:_*).show()
  }

}
