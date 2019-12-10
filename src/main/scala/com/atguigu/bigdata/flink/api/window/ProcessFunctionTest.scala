package com.atguigu.bigdata.flink.api.window

import com.atguigu.bigdata.flink.api.transform.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ProcessFunctionTest {
	def main(args: Array[String]): Unit = {

		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		//设置并行度
		env.setParallelism(1)
		//获取输入流数据信息
		val inputDataStream: DataStream[String] = env.socketTextStream("hadoop102", 9999)
		//对流进行处理
		val dataStream: DataStream[(String, String)] = inputDataStream.map {
			data => {
				val dataArray: Array[String] = data.split(",")
				SensorReading(dataArray(0).trim, dataArray(1).trim.toLong,
					dataArray(2).toDouble)
			}
		}.keyBy(_.id)
			.process(new TempIncreWaring)

		dataStream.print("data:")
		inputDataStream.print("input:")
		env.execute("process")
	}
}

class MyProcess() extends KeyedProcessFunction[String, SensorReading, Int] {
	override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, Int]#OnTimerContext, out: Collector[Int]): Unit = super.onTimer(timestamp, ctx, out)

	override def processElement(value: SensorReading,
	                            ctx: KeyedProcessFunction[String, SensorReading, Int]#Context, out: Collector[Int]): Unit = {
		ctx.timerService().registerEventTimeTimer(1000L)
		out.collect(1)

	}
}

class TempIncreWaring extends KeyedProcessFunction[String, SensorReading,
	(String, String)] {
	//保存之前的温度状态值
	lazy val lastTemp: ValueState[Double] = getRuntimeContext
		.getState(new ValueStateDescriptor[Double]
		("lastTemp", classOf[Double]))
	//保存注册定时器的时间戳
	lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer",
		classOf[Long]))


	override def processElement(value: SensorReading,
	                            ctx: KeyedProcessFunction[String, SensorReading, (String, String)]#Context, out: Collector[(String, String)]): Unit = {
		//取出上次的温度，更新状态
		val prevTemp: Double = lastTemp.value()
		lastTemp.update(value.temperature)
		//取出定时器的时间戳
		val curTimeTs: Long = currentTimer.value()

		//如果温度上升没有定时器，注册定时器
		if (value.temperature > prevTemp && curTimeTs == 0) {
			val ts: Long = ctx.timerService().currentProcessingTime() + 1000L
			ctx.timerService().registerProcessingTimeTimer(ts)
			currentTimer.update(ts)
		} //如果温度下降，删除定时器
		else if (value.temperature < prevTemp) {
			ctx.timerService().deleteProcessingTimeTimer(curTimeTs)
			currentTimer.clear()
		}

	}

	override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, (String, String)]#OnTimerContext, out: Collector[(String, String)]): Unit = {
		out.collect((ctx.getCurrentKey, "温度10s内连续上升"))
		currentTimer.clear()
	}

}
