package projectone

import java.sql.{Connection, Date, DriverManager, PreparedStatement}

import IPclassfy.{findLocation, _}
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}



class UserLocation{

}
object UserLocation {

  /**
    * 将计算后的数据存入数据库
    * @param iterator  计算后的数据（location, counts）
    */
  def dataTomysql(iterator: Iterator[(String, Int)]) ={
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "INSERT INTO location_info (location, counts, accesse_data) VALUES (? , ?, ?)"
    try{
      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?serverTimezone=GMT%2B8&useUnicode=true&characterEncoding=UTF8","root","root")
      iterator.foreach(line =>{
        ps = conn.prepareStatement(sql)
        ps.setString(1, line._1)
        ps.setInt(2, line._2)
        ps.setDate(3, new Date(System.currentTimeMillis()))
        ps.executeUpdate()
        println("Inset Successfully!")
      })
    }catch {
      case e: Exception =>{
        println(e.toString)
      }
    }finally {
      if(ps != null)
        ps.close()
      if(conn != null)
        conn.close()
    }

  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ForeachDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val RuleIps = readFile("E://Test//ip.txt")
    //广播完整规则库
    val IPBroadcast = sc.broadcast(RuleIps)
    val result= sc.textFile("E://Test//20090121000132.394251.http.format").map(t => {
      val files = t.split("\\|")
      val ipStr = files(1)
      val templocation = findLocation(ipStr,IPBroadcast.value)
      (templocation,1)
    }).reduceByKey(_ + _)
    //存入数据
    //result.foreachPartition(dataTomysql(_))
    //查询数据表信息
    val connection = () =>{
      DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?serverTimezone=GMT%2B8","root","root")
    }
    val jdbcRdd = new JdbcRDD(sc,connection,"select * from location_info where counts > ? and counts < ?",500,1800,2,re=>{
      val location = re.getString(1)
      val counts = re.getInt(2)
      (location,counts)
    })
    println(jdbcRdd.collect().toBuffer)
    sc.stop()
  }

}
