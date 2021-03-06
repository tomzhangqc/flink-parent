package table.demo

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * @author zhangqingchun
 * @date 2021/1/27
 * @description
 */
object Demo3 {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    val createSource: String =
      """
        |CREATE TABLE sourceTable (
        |`id` BIGINT,
        |`age` BIGINT,
        |`name` STRING,
        |`ts` BIGINT,
        | rt AS TO_TIMESTAMP( FROM_UNIXTIME(ts/1000) ),
        | WATERMARK FOR rt as rt - interval '3' second
        |) WITH (
        |'connector' = 'kafka-0.11',
        |'topic' = 'test',
        |'properties.bootstrap.servers' = 'localhost:9092',
        |'properties.group.id' = 'testGroup',
        |'scan.startup.mode' = 'earliest-offset',
        |'format' = 'json'
        |)
      """.stripMargin
    val createSink: String =
      """
        |CREATE TABLE sinkTable (
        |`id` BIGINT,
        |`age` BIGINT
        |) WITH (
        |'connector' = 'elasticsearch-6',
        |'hosts' = 'http://localhost:9200',
        |'document-type' = 'user',
        |'index' = 'users',
        |'format' = 'json'
        |)
      """.stripMargin
    val sql: String =
      """
        |insert into sinkTable
        |select id,age from
        |(select id,age,row_number()over(partition by name order by ts) as rownum from sourceTable)
        |where rownum = 1 """.stripMargin
    tableEnv.executeSql(createSource)
    tableEnv.executeSql(createSink)
    tableEnv.executeSql(sql)
  }
}
