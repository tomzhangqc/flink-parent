package table.demo

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import table.element.Test

import java.time.Duration

/**
 * @author zhangqingchun
 * @date 2021/1/27
 * @description
 */
object Demo1 {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val inputDataStream: DataStream[String] = env.readTextFile("/Users/winters/IdeaProjects/project/flink-parent/table-api/src/main/resources/table.txt")
    val lessonDataSet: DataStream[Test] = inputDataStream
      .map(data => {
        val words = data.split(" ")
        Test(words(0).toInt, words(1).toLong, words(2))
      })
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(new SerializableTimestampAssigner[Test]() {
        override def extractTimestamp(element: Test, recordTimestamp: Long): Long = element.time * 1000
      }))
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    val createSink: String =
      """CREATE TABLE sinkTable (
      `id` BIGINT,
      `color` STRING
    ) WITH (
      'connector' = 'kafka-0.11',
      'topic' = 'flink_sink',
      'properties.bootstrap.servers' = 'localhost:9092',
      'properties.group.id' = 'testGroup',
      'scan.startup.mode' = 'earliest-offset',
      'format' = 'json'
    )"""
    tableEnv.executeSql(createSink)
    tableEnv.createTemporaryView("sourceTable", lessonDataSet, 'id, 'time.rowtime() as 'rw, 'color)
    tableEnv.executeSql("insert into sinkTable select id,color from sourceTable where id = 1 ")
    env.execute("tableApi")
  }

}
