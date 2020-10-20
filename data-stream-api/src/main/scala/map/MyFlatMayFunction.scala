package map

import element.Student
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector

class MyFlatMayFunction extends RichFlatMapFunction[Student,Int]{

  override def flatMap(value: Student, out: Collector[Int]): Unit = {
    out.collect(10)
  }
}
