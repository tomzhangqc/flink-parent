package table.demo


import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{Table, Tumble}
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import table.element.Test

import java.time.Duration

object TableTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val inputDataStream: DataStream[String] = env.readTextFile("/Users/winters/IdeaProjects/project/flink-parent/table-api/src/main/resources/table.txt")
    val lessonDataSet: DataStream[Test] = inputDataStream
      .map(data => {
        val words = data.split(" ")
        Test(words(0).toInt, words(1).toLong)
      })
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(new SerializableTimestampAssigner[Test]() {
        override def extractTimestamp(element: Test, recordTimestamp: Long): Long = element.time * 1000
      }))
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    val dataTable: Table = tableEnv.fromDataStream(lessonDataSet, 'id, 'time.rowtime as 'rowtime)
    val resultTable = dataTable.window(Tumble over 10.seconds on 'rowtime as 'w)
      .groupBy('w, 'id)
      .select('id, 'w.end)
    resultTable.execute().collect().forEachRemaining(println)
    dataTable.printSchema()
    env.execute("tableApi")
  }
}
