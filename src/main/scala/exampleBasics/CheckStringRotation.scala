package exampleBasics

//Given two string: write a function that will return true if one string is a rotation of the other string
//eg: 'bca' and 'cab' are rotation of 'abc'
// 'barbazfoo' and 'oobarbazf' are rotation of 'foobarbaz' and it should return true.

object CheckStringRotation {

  def main(args: Array[String]): Unit = {
    print(isRotation("cab", "bca"))
  }

  def isRotation(string1 : String, string2 : String) : Boolean={
    if(string1.size != string2.size){
      return false;
    }
    var s = string2 + string2
    return s.contains(string1)
  }
}
