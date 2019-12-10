package com.atguigu.bigdata.flink.api.sink

import com.atguigu.bigdata.flink.api.transform.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object MyRedisSink {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		val inputDataStream: DataStream[String] = env.readTextFile("input")


		val dataStram = inputDataStream.map(
			data => {
				val dataArray: Array[String] = data.split(",")
				//匹配给样例类
				SensorReading(dataArray(0).trim, dataArray(1).trim.toLong,
					dataArray(2).toDouble)
			}
		)
		val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
			.setHost("hadoop102").setPort(6379).build()
		dataStram.addSink(new RedisSink[SensorReading](config,new MyRedisMapper))
		env.execute("redis sink")
	}
}

class MyRedisMapper extends RedisMapper[SensorReading] {

	override def getCommandDescription: RedisCommandDescription = {
		//hash表命名
		new RedisCommandDescription(RedisCommand.HSET,"sensor_temp")
	}

	override def getKeyFromData(t: SensorReading): String = t.temperature.toString

	override def getValueFromData(t: SensorReading): String = t.id
}
