package exampleBasics

import scala.collection.immutable.HashMap


object StringToBinaryRep  {

  val userDetails = HashMap("1" -> "ab",
    "2" -> "abc",
    "3" -> "abd",
    "4" -> "ab")

  val lettersToCheck = "abcd"
  def main(args: Array[String]): Unit = {



    getBinaryRepresentation foreach ( (t2) => println (t2._1 + "-->" + t2._2))

  }

  def getBinaryRepresentation: Map[String, String] = userDetails.mapValues(
    string => lettersToCheck.map(
      letter => if (string.contains(letter)) '1' else '0'))

/*output
* 4-->1100
1-->1100
2-->1110
3-->1101
* */
}