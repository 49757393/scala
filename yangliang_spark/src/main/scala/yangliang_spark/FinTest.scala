package yangliang_spark
import java.sql.{ Connection, DriverManager, ResultSet }
import java.util.Properties
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode,Dataset,Row}
import org.apache.spark.sql.SparkSession
 
object FinTest {
  
  
  def main(args: Array[String]) {
    //classOf[org.postgresql.Driver]
    val spark = SparkSession
          .builder
          .appName("Fin Test")
          .getOrCreate()
    import spark.implicits._


    System.out.println("确保数据库已经开启，并创建了products表和插入了数据");
    val prop = new java.util.Properties


        //增加数据库的用户名(user)密码(password),指定postgresql驱动(driver)
    System.out.println("增加数据库的用户名(user)密码(password),指定postgresql驱动(driver)");
    prop.setProperty("driver","org.postgresql.Driver")
    prop.setProperty("user", "yangliang")
    prop.setProperty("password", "123456")
    val url = "jdbc:postgresql://127.0.0.1:5432/exampledb"
    var sql = "(select a.acc,b.amt from  brch_qry_dtl_2 a ,brch_qry_dtl b where a.acc='6042****5274' and a.acc=b.acc) t"
    //var sql = "select a.acc,b.amt from  brch_qry_dtl_2 a ,brch_qry_dtl b where a.acc='6042****5274' and a.acc=b.acc"
    val  df  = spark.read.jdbc(url, sql,prop).select("acc","amt")
    println(df.show())    
    



        //df,append模式是连接模式，默认的是"error"模式。
    //df.write().mode(SaveMode.Overwrite).jdbc("jdbc:postgresql://localhost:5432/postgres","brch_qry_dtl_r",prop);

    spark.close();
  }
}