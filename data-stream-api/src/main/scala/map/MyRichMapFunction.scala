package map

import element.Student
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}

class MyRichMapFunction extends RichMapFunction[Student, Int] {

  lazy val maxAge: ValueState[Int] = getRuntimeContext.getState(new ValueStateDescriptor("max-age", classOf[Int]))

  override def map(value: Student): Int = {
    val age = maxAge.value()
    println(age)
    if (value.age > age) {
      maxAge.update(value.age)
    }
    value.age.max(age)
  }

}
