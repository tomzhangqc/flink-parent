package table.demo


import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{Table, Tumble}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row
import table.element.Test

object TableTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val inputDataStream: DataStream[String] = env.readTextFile("/Users/IdeaProjects/project/flink-demo/parent/table-api/src/main/resources/table.txt")
    val lessonDataSet: DataStream[Test] = inputDataStream
      .map(data => {
        val words = data.split(" ")
        Test(words(0).toInt, words(1).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Test](Time.seconds(1)) {
        override def extractTimestamp(element: Test): Long = {
          element.time * 1000
        }
      })
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    val dataTable: Table = tableEnv.fromDataStream(lessonDataSet, 'id, 'time.rowtime as 'rowtime)
    val resultTable = dataTable.window(Tumble over 10.seconds on 'rowtime as 'w)
      .groupBy("w,id")
      .select("id,w.end")
    resultTable.toRetractStream[Row].print("stream table")
    dataTable.printSchema()
    env.execute("stream word count job")
  }
}
