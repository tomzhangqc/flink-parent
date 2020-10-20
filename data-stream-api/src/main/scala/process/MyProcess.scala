package process

import element.Student
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

class MyProcess (timer: Long) extends KeyedProcessFunction[Int, Student, String] {

  lazy val lastAgeState: ValueState[Int] = getRuntimeContext.getState(new ValueStateDescriptor[Int]("lastAge", classOf[Int]))

  lazy val currentTimerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("currentTimer", classOf[Long]))

  override def processElement(value: Student, ctx: KeyedProcessFunction[Int, Student, String]#Context, out: Collector[String]): Unit = {
    val lastAge = lastAgeState.value()
    val currentTimer = currentTimerState.value()
    lastAgeState.update(value.age)
    if (value.age > lastAge && currentTimer == 0) {
      val ts = ctx.timerService().currentProcessingTime() + timer
      ctx.timerService().registerProcessingTimeTimer(ts)
      currentTimerState.update(ts)
    }
    else if (value.age < lastAge) {
      ctx.timerService().deleteProcessingTimeTimer(currentTimer)
      currentTimerState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Int, Student, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect("年龄上升")
    currentTimerState.clear()
  }
}