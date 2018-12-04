package projectone

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io._

object IPclassfy {
//  def main(args: Array[String]): Unit = {
//    val IpArray = readFile("E://Test//ip.txt")
//    val to2Ip = ip2Long("112.18.22.43" )
//    println(to2Ip)
//    val endIndex = binarySearch(to2Ip, IpArray)
//    println(IpArray(endIndex))
//  }

  /**
    * 根据ip查找归属地
    * @param ip
    * @param IpArray
    * @return  (location)
    */
  def findLocation(ip: String, IpArray: ArrayBuffer[String]): (String) ={
    val to2Ip = ip2Long(ip)
    val findIndex = binarySearch(to2Ip, IpArray)
    val location = IpArray(findIndex).split("\\|")(6)
    (location)
  }
  /**
    * 将文件内容转换成一个ArrayBuffer[String]
    * @param path  文件目录
    * @return   ArrayBuffer[String]
    */
  def  readFile(path: String) : ArrayBuffer[String] = {
    var array = mutable.ArrayBuffer[String]()
    val files = Source.fromFile(path)
    for (lines <- files.getLines()){
      array += lines
    }
    array

  }

  /**
    * 八进制转10进制
    * @param ip 八进制 IP
    * @return 十进制 IP
    */
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length){
      ipNum =  fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  /**
    * 二分法查找
    * @param ConpareL 目标值
    * @param lines 查询源
    * @return 目标Index
    */
  def binarySearch(ConpareL :Long, lines: ArrayBuffer[String]) :Int = {
    var low = 0
    var high =lines.length - 1
    while(low <= high){
      val model =  (low+high) / 2
      val min = lines(model).split("\\|")(2).toLong
      val max = lines(model).split("\\|")(3).toLong
      if((ConpareL >= min) && (ConpareL <= max)){
        return model
      }
      if(ConpareL < min){
        high = model - 1
      }else{
        low = model + 1
      }
    }
    -1
  }

}

