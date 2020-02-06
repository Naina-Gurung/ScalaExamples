package exampleBasics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object ArraysRDD {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  def main(args: Array[String]): Unit = {

    val conf= new SparkConf()
    conf.setAppName("Word Count")
    conf.setMaster("local")

    //create a spark context object
    val sc = new SparkContext(conf)

    val x = sc.parallelize( List( ("A", Set(1,2)) ) )
    val x2 = sc.parallelize( List( ("A", Set(3,4)) ) )

    val arr = Array(x,x2)

    val res=arr.reduce(_++_).map(x=>(x._1,x._2.mkString(",")))
    res.foreach(println)
    //(A,1,2)
    //(A,3,4)

    //arr.reduce(_++_).groupByKey.mapValues(_.flatMap(identity)).foreach(println)
    //(A,List(1, 2, 3, 4))



  }

}
