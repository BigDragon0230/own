import java.io.{File, FileInputStream, InputStream}
import java.util.Properties

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Try
import scala.util.parsing.json.JSON

object Kafka2Spark2Hbase {

  Logger.getLogger("com").setLevel(Level.ERROR)
  Logger.getLogger("org").setLevel(Level.WARN)

  private val confPath: String = System.getProperty("user.dir") + File.separator + "conf"
  println("配置文件路径：" + confPath)

  def main(args: Array[String]): Unit = {

   /* //验证程序参数
    if (args.length != 0) {
      print(
        """
          |Kafka2Spark2Hbase
          |参数个数不匹配！！！
          |所需参数：
          |path1 path2  path3
        """.stripMargin)
      System.exit(0)
    }
    //接收参数
    val Array(path1, path2, path3) = args*/

    //加载配置文件
    val prop: Properties = new Properties()
    val file: File = new File(confPath + File.separator + "kafka.properties")
    if (!file.exists()) {
      val in: InputStream = Kafka2Spark2Hbase.getClass.getClassLoader.getResourceAsStream("kafka.properties")
      prop.load(in)
    } else {
      prop.load(new FileInputStream(file))
    }

    //读取配置文件内容
    val brokers: String = prop.getProperty("kafka.brokers")
    val topics: String = prop.getProperty("kafka.topics")
    val groupId: String = prop.getProperty("group.id")
    println("kafka.brokers:" + brokers)
    println("kafka.topics:" + topics)
    println("group.id:" + groupId)

    if (StringUtils.isEmpty(brokers) || StringUtils.isEmpty(topics) || StringUtils.isEmpty(groupId)) {
      println("未配置Kafka信息")
      System.exit(0)
    }

    //topic放入set中
    val topicsSet: Set[String] = topics.split(",").toSet

    //创建sparkseesion
    val session: SparkSession = SparkSession.builder()
      .appName(s"${this.getClass.getSimpleName}")
      .master("local[*]")
      .config(new SparkConf())
      .getOrCreate()
    //创建ssc
    val ssc: StreamingContext = new StreamingContext(session.sparkContext, Seconds(5)) //设置Spark时间窗口，每5s处理一次
    //配置kafka参数
    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "auto.oggset.reset" -> "latest",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group,id" -> groupId
    )

    //创建实时流
    val dstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)
    )

    dstream.foreachRDD(rdd => {
      rdd.foreachPartition(par => {
        val conn: Connection = HBaseUtils.getNoKBHBaseCon(confPath) // 每一个分区创建一个连接，获取Hbase连接
        par.foreach(line => {
          //将Kafka的每一条消息解析为JSON格式数据
          println("每条日志：" + line.value())
          val jsonObj: Option[Any] = JSON.parseFull(line.value())
          val map: Map[String, Any] = jsonObj.get.asInstanceOf[Map[String, Any]]//将json数据转换为map格式
          val rowkey: String = map.get("id").asInstanceOf[String]
          val name: String = map.get("name").asInstanceOf[String]
          val city: String = map.get("city").asInstanceOf[String]

          //对应hbase表
          val tableName: TableName = TableName.valueOf("hbasetable")//声明表名
          val table: Table = conn.getTable(tableName)//连接中获取表名
          val put: Put = new Put(Bytes.toBytes(rowkey))
          put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(name))
          put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("city"),Bytes.toBytes(city))

          Try(table.put(put)).getOrElse(table.close())//将数据写入HBase，若出错关闭table
          table.close()//分区数据写入HBase后关闭连接
        })
        conn.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
