package demo

import org.apache.flink.streaming.api.scala._

object Bachend {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream = env.socketTextStream("localhost", 7777)
    val dataStream: DataStream[String] = inputStream
      .map(data => {
        val dataArray = data.split(" ")
        dataArray(0)
      })
    dataStream.print()
    env.execute("stream word count job")
  }

}
