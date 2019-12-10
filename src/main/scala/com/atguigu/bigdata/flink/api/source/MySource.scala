package com.atguigu.bigdata.flink.api.source

import com.atguigu.bigdata.flink.api.transform.SensorReading
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.util.Random

object MySource {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		val myCustomerSource: DataStream[SensorReading] = env.addSource(new MyCustomerSource)
		myCustomerSource.print()
		env.execute()
	}
}

//自定义数据源
class MyCustomerSource extends SourceFunction[SensorReading] {


	// flag: 表示数据源是否还在正常运行
	var runing: Boolean = true

	//利用高斯分布来产生测试数据
	override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
		//设置一个初始值
		//初始化一个随机数发生器
		val random: Random = new Random()
		var currentTemp = 1.to(10).map(
			current => ("sensor_" + current, (random.nextGaussian() * 20 + 60))
		)
		//更新温度
		while (runing) {
			currentTemp = currentTemp.map(
				temp => (temp._1, temp._2 + random.nextGaussian())
			)
			//获取当前的时间戳
			val time: Long = System.currentTimeMillis()

			currentTemp.foreach(t => ctx.collect(SensorReading(t._1, time, t._2)))
			Thread.sleep(1000)
		}
	}

	override def cancel(): Unit = {
		runing = false
	}
}
