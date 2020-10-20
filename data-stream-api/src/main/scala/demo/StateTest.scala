package demo

import element.Student
import map.MyFlatMayFunction
import org.apache.flink.streaming.api.scala._

object StateTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream = env.socketTextStream("localhost", 7777)
    val resultDataSet: DataStream[Int] = inputStream
      .map(data => {
        val words = data.split(" ")
        Student(words(0).toInt, words(1), words(2).toInt,words(3).toLong)
      })
      .keyBy(_.id)
      //      .map(new MyRichMapFunction())
      .flatMap(new MyFlatMayFunction())
    //      .process(new MyProcess(10000l))
    resultDataSet.print()
    env.execute("stream word count job")
  }
}
