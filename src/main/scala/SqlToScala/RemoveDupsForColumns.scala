package SqlToScala

import java.util

import scala.io.Source
import scala.util.Sorting.quickSort


object RemoveDupsForColumns {

  def main(args: Array[String]): Unit = {


    val column = new Array[String](2);
    val arrlist = new util.ArrayList[String]()

    val filename = "/Users/z002gh2/naina/GITREPO/WorkspaceLearning/LearningScala/src/main/scala/SqlToScala/data.csv"
    for (line <- Source.fromFile(filename).getLines) {
      column(0) = line.split(",")(0)
      column(1) = line.split(",")(1)

      quickSort(column)

      val newLine = column(0) + "," + column(1)
      if(!arrlist.contains(newLine)){
        arrlist.add(newLine)
      }

    }

    for (index <- 0 until(arrlist.size())){
      println(arrlist.get(index))
    }

  }
}

/* input:
a_name,b_name
dorry,madhu
madhu,dorry
dorry,nemo
nemo,dorry
dorry,usher
madhu,nemo
nemo,madhu
usher,dorry
madhu,usher
usher,madhu
nemo,usher
usher,nemo

output:
a_name,b_name
dorry,madhu
dorry,nemo
dorry,usher
madhu,nemo
madhu,usher
nemo,usher
 */
