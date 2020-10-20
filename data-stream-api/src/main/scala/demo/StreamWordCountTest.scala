package demo

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object StreamWordCountTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val inputStream = env.socketTextStream("localhost", 7777)
    val resultDataSet: DataStream[(String, Int)] = inputStream
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      //            .assignAscendingTimestamps(_._2)//有序waterMaker
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Int)](Time.seconds(1)) {
      override def extractTimestamp(element: (String, Int)): Long = {
        1000l
      }
    }) //无序waterMaker
      .keyBy(0)
      .timeWindow(Time.seconds(10), Time.seconds(5))
      //      .allowedLateness(Time.seconds(1))//允许延迟关闭窗口
      //      .sideOutputLateData(new OutputTag[(String, Int)]("late"))//延迟数据输出到侧输出流
      .sum(1)
    resultDataSet.print()
    //    resultDataSet.getSideOutput(new OutputTag[(String, Int)]("late"))//获取侧输出流
    env.execute("stream word count job")
  }
}
