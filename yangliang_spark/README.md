spark连接pg 并查询数据
spark = SparkSession.builder.master("local").appName("spark-oracle").getOrCreate()
print(spark)
sc=spark.sparkContext
print(sc)

#从oracle 数据库表中读取数据到spark，如果要使用sql语句，则必须添加别名alias，比如下面的t
sqltext="(select col1,col2,col3 from tablename) t"
jdbcdf = spark.read.format('jdbc').options(
 url='jdbc:oracle:thin:@//127.0.0.1/oa3',
    dbtable='OA3DATA.SYS_NEWS_MAIN',
    user='123',
    password='123',
    driver="oracle.jdbc.OracleDriver").load()
print(jdbcdf)
jdbcdf.createOrReplaceTempView('tmp_vw')

df = spark.sql("SELECT fd_id, doc_subject, doc_en_subject, fd_news_source, doc_publish_time, doc_content, doc_en_content, fd_html_content FROM tmp_vw")
df.show()
sc.stop()
————————————————
版权声明：本文为CSDN博主「银灯玉箫」的原创文章，遵循 CC 4.0 BY-SA 版权协议，转载请附上原文出处链接及本声明。
原文链接：https://blog.csdn.net/lilele12211104/java/article/details/100700773


val prop = new java.util.Properties


        //增加数据库的用户名(user)密码(password),指定postgresql驱动(driver)
    System.out.println("增加数据库的用户名(user)密码(password),指定postgresql驱动(driver)");
    prop.setProperty("driver","org.postgresql.Driver")
    prop.setProperty("user", "yangliang")
    prop.setProperty("password", "123456")
    val url = "jdbc:postgresql://127.0.0.1:5432/exampledb"
    var sql = "brch_qry_dtl"
    //var sql = "select a.acc,b.amt from  brch_qry_dtl_2 a ,brch_qry_dtl b where a.acc='6042****5274' and a.acc=b.acc"
    //val df = spark.read.jdbc(url, sql,prop)