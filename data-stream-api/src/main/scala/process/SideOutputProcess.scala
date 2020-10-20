package process

import element.Student
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

class SideOutputProcess (age: Int) extends ProcessFunction[Student, Student] {

  override def processElement(value: Student, ctx: ProcessFunction[Student, Student]#Context, out: Collector[Student]): Unit = {
    if (value.age > age) {
      out.collect(value)
    } else {
      ctx.output(new OutputTag[Student]("low-tag"), value)
    }
  }
}