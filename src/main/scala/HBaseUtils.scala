import java.io.{File, InputStream}
import java.security.PrivilegedAction

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.security.UserGroupInformation


object HBaseUtils {
  //ssssss

  /**
    * 获取Kerberos环境下的HBase连接
    * @param confPath
    * @param principal
    * @param keytabPath
    * @return
    */
  def getHBaseConn(confPath: String, principal: String, keytabPath: String): Connection = {
    val configuration = HBaseConfiguration.create
    val coreFile = new File(confPath + File.separator + "core-site.xml")
    if(!coreFile.exists()) {
      val in = HBaseUtils.getClass.getClassLoader.getResourceAsStream("hbase-conf/core-site.xml")
      configuration.addResource(in)
    }
    val hdfsFile = new File(confPath + File.separator + "hdfs-site.xml")
    if(!hdfsFile.exists()) {
      val in = HBaseUtils.getClass.getClassLoader.getResourceAsStream("hbase-conf/hdfs-site.xml")
      configuration.addResource(in)
    }
    val hbaseFile = new File(confPath + File.separator + "hbase-site.xml")
    if(!hbaseFile.exists()) {
      val in = HBaseUtils.getClass.getClassLoader.getResourceAsStream("hbase-conf/hbase-site.xml")
      configuration.addResource(in)
    }

    UserGroupInformation.setConfiguration(configuration)
    UserGroupInformation.loginUserFromKeytab(principal, keytabPath)
    val loginUser = UserGroupInformation.getLoginUser
    loginUser.doAs(new PrivilegedAction[Connection] {
      override def run(): Connection = ConnectionFactory.createConnection(configuration)
    })
  }


  /**
    * 获取非Kerberos环境的Connection
    * @param confPath
    * @return
    */
  /*
  * 添加访问HBase的集群配置信息hdfs-site.xml/core-stie.xml/hbase-site.xml文件
  * resources/hbase-conf/
  *
  * */

  def getNoKBHBaseCon(confPath: String): Connection = {

    //加载配置文件core-site.xml/hdfs-site.xml/hbase-site.xml
    val configuration: Configuration = HBaseConfiguration.create()
    val coreFile: File = new File(confPath + File.separator + "core-site.xml")
    if (!coreFile.exists()) {
      val in: InputStream = HBaseUtils.getClass.getClassLoader.getResourceAsStream("hbase-conf/core-site.xml")
      configuration.addResource(in)
    }
    val hdfsFile: File = new File(confPath + File.separator + "hdfs-site.xml")
    if (!hdfsFile.exists()) {
      val in: InputStream = HBaseUtils.getClass.getClassLoader.getResourceAsStream("hbase-conf/hdfs-site.xml")
      configuration.addResource(in)
    }
    val hbaseFile: File = new File(confPath + File.separator + "hbase-site.xml")
    if (!hbaseFile.exists()) {
      val in: InputStream = HBaseUtils.getClass.getClassLoader.getResourceAsStream("hbase-conf/hbase-site.xml")
      configuration.addResource(in)
    }

    //创建连接
    val conn = ConnectionFactory.createConnection(configuration)
    println("-------------" + conn.getAdmin.listTableNames().size)
    conn
  }





}
