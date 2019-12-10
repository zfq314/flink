package com.atguigu.bigdata.flink.api.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.atguigu.bigdata.flink.api.transform.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

object MyJdbcSink {
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

		dataStram.addSink(new JdbcSink())
		env.execute("jdbc sink")
	}
}

class JdbcSink extends RichSinkFunction[SensorReading] {
	var connection: Connection = _
	var insertStmt: PreparedStatement = _
	var updateStmt: PreparedStatement = _


	override def open(parameters: Configuration): Unit = {
		super.open(parameters)
		//创建连接
		connection = DriverManager.getConnection("jdbc:mysql://hadoop103:3306/test", "root", "root")
		//执行sql
		insertStmt = connection.prepareStatement("INSERT INTO temperatures (sensor, temp) VALUES (?, ?)")
		updateStmt = connection.prepareStatement("UPDATE temperatures SET temp = ? WHERE sensor = ?")
	}


	override def invoke(value: SensorReading, context: SinkFunction
	.Context[_]): Unit = {
		updateStmt.setDouble(1, value.temperature)
		updateStmt.setString(2, value.id)
		updateStmt.execute()
		if (updateStmt.getUpdateCount == 0) {
			insertStmt.setString(1, value.id)
			insertStmt.setDouble(2, value.temperature)
			insertStmt.execute()
		}
	}

	override def close(): Unit = {
		insertStmt.close()
		updateStmt.close()
		connection.close()
	}

}