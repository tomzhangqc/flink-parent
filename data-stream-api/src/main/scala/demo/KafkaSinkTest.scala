package demo

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream = env.readTextFile("")
    val dataStream: DataStream[String] = inputStream
      .map(data => {
        val dataArray = data.split(" ")
        Map(dataArray(0) -> dataArray(1)).toString()
      })
    dataStream.print()
    dataStream.addSink(new FlinkKafkaProducer[String]("localhost:9092","kafka-flink",new SimpleStringSchema()))
    env.execute("kafka sink test")
  }

}
