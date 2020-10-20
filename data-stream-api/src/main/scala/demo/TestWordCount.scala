package demo

import element.Lesson
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object TestWordCount {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val inputStream = env.socketTextStream("localhost", 7777)
    val lessonDataSet: DataStream[Lesson] = inputStream
      .map(data => {
        val words = data.split(" ")
        Lesson(words(0).toInt, words(1).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Lesson](Time.seconds(1)) {
        override def extractTimestamp(element: Lesson): Long = {
          element.time * 1000l
        }
      })
    lessonDataSet.keyBy(_.id)
      .timeWindow(Time.seconds(10), Time.seconds(5))
      .process(new ProcessWindowFunction[Lesson, (Long, Int), Int, TimeWindow] {
        override def process(key: Int, context: Context, elements: Iterable[Lesson], out: Collector[(Long, Int)]): Unit = {
          out.collect(context.currentWatermark, elements.size)
        }
      }).print("window test")
    env.execute("stream word count job")
  }
}
