package exampleBasics

// given a list of integers ( can be positive and negative)
// {-2,-3,4,-1,-2,1,5,-3}
// 4+ (-1) + (-2) + 1+ 5 = 7
//maximum contiguous array sum is 7

object LargestSumContiguousSubarray {

  def main(args: Array[String]): Unit = {

    val arrStr = Array(-2,-3,4,-1,-2,1,5,-3)


    print(maxContiguoussum(arrStr,arrStr.size))
  }

  def maxContiguoussum(arrStr : Array[Int], len : Int ) : Integer={

    var max_so_far : Int = 0;
    var max_ending_here : Int = 0 ;

    for(i <- 0 until len){
      max_ending_here = max_ending_here + arrStr(i)
      if(max_ending_here < 0 ){
        max_ending_here = 0;
      }
      else if (max_so_far<max_ending_here){
        max_so_far= max_ending_here;
      }
    }
    return max_so_far
  }
}
