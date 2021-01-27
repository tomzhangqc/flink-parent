package blink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment

object BlinkDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val blinkSettings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, blinkSettings)
    val createTable: String =
      """
        |create table demo1(
        | id INT COMMENT 'ID',
        | name VARCHAR COMMENT '姓名',
        | age INT COMMENT '年龄',
        | ts BIGINT COMMENT '时间',
        | rt AS TO_TIMESTAMP( FROM_UNIXTIME(ts/1000) ),
        | WATERMARK FOR rt as rt - interval '10' second
        |)
        |WITH(
        | 'connector.type' = 'kafka',
        | 'connector.version' = '0.11',
        | 'connector.topic' = 'test',
        | 'connector.startup-mode' = 'latest-offset',
        | 'connector.properties.0.key' = 'zookeeper.connect',
        | 'connector.properties.0.value' = 'localhost:2181',
        | 'connector.properties.1.key' = 'bootstrap.servers',
        | 'connector.properties.1.value' = 'localhost:9092',
        | 'update-mode' = 'append',
        | 'format.type' = 'json',
        | 'format.derive-schema' = 'true'
        |)
      """.stripMargin
    //    val createSinkTable: String =
    //      """
    //        |create table demo2(
    //        | id INT COMMENT 'ID',
    //        | age INT COMMENT 'age'
    //        |)
    //        |WITH(
    //        | 'connector.type' = 'elasticsearch',
    //        | 'connector.version' = '6',
    //        | 'connector.hosts' = 'http://localhost:9200',
    //        | 'connector.index' = 'flink',
    //        | 'connector.document-type' = 'demo',
    //        | 'connector.bulk-flush.max-actions' = '1',
    //        | 'format.type' = 'json',
    //        | 'update-mode' = 'upsert'
    //        |)
    //      """.stripMargin
    tableEnv.executeSql(createTable)
    //    tableEnv.sqlUpdate(createSinkTable)
    //    val sql = "insert into demo2 select id,TUMBLE_START(rt, INTERVAL '1' SECOND) as window_start," +
    //      "TUMBLE_END(rt, INTERVAL '1' SECOND) as window_end from demo1 GROUP BY TUMBLE(rt, INTERVAL '1' SECOND), id"
    //    val sql = "insert into demo2 select id,max(age) from demo1 GROUP BY id"
    //    tableEnv.sqlUpdate(sql)
    val sql = "select * from demo1"
    val result = tableEnv.sqlQuery(sql)
    result.execute().collect().forEachRemaining(println)
    //    result.printSchema()
    //    val dataStream = tableEnv.toRetractStream(result, classOf[Row]);
    //    dataStream.print()
    tableEnv.execute("Flink SQL DDL")
  }

}
