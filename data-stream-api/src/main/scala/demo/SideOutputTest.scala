package demo

import element.Student
import org.apache.flink.streaming.api.scala._
import process.SideOutputProcess

object SideOutputTest {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream = env.socketTextStream("localhost", 7777)
    val dataStream: DataStream[Student] = inputStream
      .map(data => {
        val words = data.split(" ")
        Student(words(0).toInt, words(1), words(2).toInt,words(3).toLong)
      })
    val highStream = dataStream.process(new SideOutputProcess(20))
    val lowStream = highStream.getSideOutput(new OutputTag[Student]("low-tag"))
    highStream.print("high")
    lowStream.print("low")
    env.execute("stream word count job")
  }
}

