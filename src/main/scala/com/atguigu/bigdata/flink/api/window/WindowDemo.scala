package com.atguigu.bigdata.flink.api.window

import java.util

import com.atguigu.bigdata.flink.api.transform.SensorReading
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.streaming.api.environment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, WindowAssigner}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object WindowDemo {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

		//val inputDataStream: DataStream[String] = env.readTextFile("input")
		val inputDataStream: DataStream[String] = env.socketTextStream("hadoop102", 9999)

		val dataStram = inputDataStream.map(
			data => {
				val dataArray: Array[String] = data.split(",")
				//匹配给样例类
				SensorReading(dataArray(0).trim, dataArray(1).trim.toLong,
					dataArray(2).toDouble)
			}
		)
			.map(data => (data.id, data.temperature))
			.keyBy(_._1)
			//.timeWindow(Time.seconds(15))
			//.window(new MyWindowAssginer())
			//.window(TumblingEventTimeWindows.of( Time.minutes(5)))
			//.timeWindow(Time.hours(1),Time.minutes(5))
			.window(EventTimeSessionWindows.withGap(Time.minutes(1)))
			.reduce((result, newDate) => (result._1, result._2.min(newDate._2)))
		dataStram.print()
		env.execute("window")
	}
}

class MyWindowAssginer() extends WindowAssigner[SensorReading, TimeWindow] {
	override def assignWindows(element: SensorReading, timestamp: Long, context: WindowAssigner.WindowAssignerContext): util.Collection[TimeWindow] = ???

	override def getDefaultTrigger(env: environment.StreamExecutionEnvironment): Trigger[SensorReading, TimeWindow] = ???

	override def getWindowSerializer(executionConfig: ExecutionConfig): TypeSerializer[TimeWindow] = ???

	override def isEventTime: Boolean = ???
}