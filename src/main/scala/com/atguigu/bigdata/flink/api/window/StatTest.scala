package com.atguigu.bigdata.flink.api.window

import com.atguigu.bigdata.flink.api.transform.SensorReading
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object StatTest {
	def main(args: Array[String]): Unit = {

		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		//设置并行度
		env.setParallelism(1)

		//获取输入流数据信息
		val inputDataStream: DataStream[String] = env.socketTextStream("hadoop102", 9999)
		//对流进行处理
		val dataStream: KeyedStream[SensorReading, String] = inputDataStream.map {
			data => {
				val dataArray: Array[String] = data.split(",")
				SensorReading(dataArray(0).trim, dataArray(1).trim.toLong,
					dataArray(2).toDouble)
			}
		}
			.keyBy(_.id)
		//自定义处理状态
		val first = dataStream.flatMap(new TempChanWarning(10.0))
		//用框架来处理状态
		val second: DataStream[(String, Double, Double)] = dataStream.flatMapWithState[(String, Double, Double), Double] {
			case (inputData: SensorReading, None) => (List.empty, Some(inputData.temperature))
			case (inputData: SensorReading, lastTemp: Some[Double]) =>
				val diff = (inputData.temperature - lastTemp.get).abs
				if (diff > 10.0) {
					(List((inputData.id, lastTemp.get, inputData.temperature)), Some
					(inputData.temperature))
				} else
					(List.empty, Some(inputData.temperature))
		}

		inputDataStream.print("input:")

		first.print("warning1:")

		second.print("warning2:")

		env.execute("stat test")
	}
}

class TempChanWarning(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {
	//定义状态变量，用于保存上次温度值
	private var lastTempStat: ValueState[Double] = _
	//判断是否是首次，如果是首次的状态，不报警,默认的结果是false不是第一次，所以只有结果是true的时候才是第一次
	private var isNotFirst: ValueState[Boolean] = _

	override def open(parameters: Configuration): Unit = {
		lastTempStat = getRuntimeContext.getState[Double](new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
		isNotFirst = getRuntimeContext.getState[Boolean](new ValueStateDescriptor[Boolean]("isNotFirst", classOf[Boolean]))
	}

	override def flatMap(value: SensorReading, out: Collector[(String,
		Double, Double)]): Unit = {
		//拿到上一次的温度值
		val preTemp: Double = lastTempStat.value()
		val notFirst: Boolean = isNotFirst.value()
		//和上次的温度值比较
		val diff = (value.temperature - preTemp).abs
		if (notFirst && diff > threshold) {
			//进行报警
			out.collect(value.id, preTemp, value.temperature)
		}
		//更新状态
		lastTempStat.update(value.temperature)
		isNotFirst.update(true)
	}
}