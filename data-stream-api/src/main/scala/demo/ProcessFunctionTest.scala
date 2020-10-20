package demo

import element.Student
import org.apache.flink.streaming.api.scala._
import process.MyProcess

object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream = env.socketTextStream("localhost", 7777)
    val resultDataSet: DataStream[String] = inputStream
      .map(data => {
        val words = data.split(" ")
        Student(words(0).toInt, words(1), words(2).toInt, words(3).toLong)
      })
      .keyBy(_.id)
      .process(new MyProcess(10000l))
    resultDataSet.print()
    env.execute("stream word count job")
  }

}
