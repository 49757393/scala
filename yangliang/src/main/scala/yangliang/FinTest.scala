package yangliang
import java.sql.{ Connection, DriverManager, ResultSet };
import java.util.Properties;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.{SaveMode,Dataset,Row};
import org.apache.spark.sql.SparkSession
 
object FinTest {
  
  val conn_str = "jdbc:postgresql://localhost:5432/exampledb"
  Class.forName("org.postgresql.Driver").newInstance
  def main(args: Array[String]) {
    //classOf[org.postgresql.Driver]
 
    val conn = DriverManager.getConnection(conn_str, "yangliang", "123456")
    try {
      // Configure to be Read Only
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
 
      // Execute Query
      val rs = statement.executeQuery("SELECT * FROM brch_qry_dtl")
      var columnCount = rs.getMetaData().getColumnCount();
      System.out.print(columnCount + "\t");
      // Iterate Over ResultSet
      while (rs.next) {
       println("acc====="+rs.getString("acc")+",tran_date====="+rs.getString("tran_date"))
     }
    } finally {
      conn.close
    }
  }
}