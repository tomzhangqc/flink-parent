package demo

import java.util
import java.util.Properties
import com.google.gson.Gson
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Duration

object LoginCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("key.serializer", classOf[StringDeserializer].getName)
    properties.setProperty("value.serializer", classOf[StringDeserializer].getName)
    properties.setProperty("group.id", "flink-cep")
    properties.setProperty("auto.offset.reset", "latest")
    val stream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("cep_test", new SimpleStringSchema(), properties))
    //    val stream: DataStream[String] = env.socketTextStream("localhost", 7777)
    val inputStream: DataStream[Login] = stream
      .map(data => {
        val gson = new Gson
        gson.fromJson(data, classOf[Login])
      })
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(new SerializableTimestampAssigner[Login]() {
        override def extractTimestamp(element: Login, recordTimestamp: Long): Long = element.time
      }))
    val loginPattern: Pattern[Login, Login] = Pattern
      .begin[Login]("first")
      .where(_.status == "fail")
      .times(2)
      .consecutive()
    //      .within(Time.seconds(10))
    val patternStream: PatternStream[Login] = CEP.pattern(inputStream.keyBy(_.id), loginPattern)
    val result: DataStream[Fail] = patternStream.select(new FailLogin())
    result.print("result")
    env.execute("login cep")
  }
}

case class Login(id: Int, status: String, time: Long) {

}

case class Fail(id: Int, time: Long) {

}

class FailLogin extends PatternSelectFunction[Login, Fail] {
  override def select(map: util.Map[String, util.List[Login]]): Fail = {
    val first = map.get("first").get(0)
    Fail(first.id, first.time)
  }
}
