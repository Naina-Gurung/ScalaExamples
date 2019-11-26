package exampleBasics

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import scala.collection.mutable.ListBuffer




object Employee {

  case class ItemInfo(item:String, quantity:Int)

 // case class Employee(id: Int, name: String, age: Int)
  //case class Employee( factor: String,rank: String)

  def main(args: Array[String]): Unit = {

    val conf= new SparkConf()
    conf.setMaster("local")
    conf.setAppName("master")
    val sc= new SparkContext(conf)

   //val rdd=sc.textFile(getClass.getResource("/rank.txt").getPath)
    val spark = SparkSession.builder().appName("ranking").getOrCreate()


    import spark.implicits._

   val data = sc.parallelize(List(("a",10),("b",20),("c",30)))
    data.collect().foreach(println)
    val ItemDF = data.map(x=> ItemInfo(x._1,x._2)).toDF()
    ItemDF.createOrReplaceTempView("Item_tbl")
    val rankedItems = spark.sql("select item, quantity, rank() over(order by quantity desc) as rank from Item_tbl")
    rankedItems.collect().foreach(println)


  /*  val rdd1 = sc.parallelize(Array((1,2),(2,3),(1,3),(2,4)))
    val gRdd = rdd1.groupByKey()
    val indxRdd = gRdd.mapValues(a => {
      val b = a.toArray
      var indx = 2
      val lb = new ListBuffer[(Int, Int)]
      for(i <- 0 to b.size-1) {
        lb.append((b(i), indx))
        indx += 1
      }
      lb.toArray
    })

    indxRdd.collectAsMap().foreach(println)
    (1,2)
    (1,3)
    (2,3)
    (2,4)*/

  }


}
