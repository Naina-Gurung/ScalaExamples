package exampleBasics

object StringRepeat {

  def main(args: Array[String]): Unit = {
    print(isRepeated("dogdogdog"));
  }

  def isRepeated(str: String):Boolean={
    var s= str+str;
    var len=s.length

    return s.substring(1,len-1).contains(str);
  }
}
