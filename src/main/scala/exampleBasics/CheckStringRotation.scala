package exampleBasics


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
