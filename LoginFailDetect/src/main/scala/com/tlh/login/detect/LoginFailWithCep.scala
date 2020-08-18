package com.tlh.login.detect

import java.util

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @Comment 登陆失败检测最终版 CEP（Complex Event Processing，复杂事件处理）
 * @Author: tlh
 * @Date 2020/8/17 16:20
 * @Version 1.0
 */
object LoginFailWithCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度,根据环境决定,设置为1便于测试
    env.setParallelism(1)
    //设置事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //数据源
    val resource = getClass.getResource("/LoginLog.csv")
    val inputStream = env.readTextFile(resource.getPath)

    //转换样例类,提取时间戳,设置watermark(处理乱序数据时)
    val loginEventStream = inputStream
      .map(
        data => {
          val arr = data.split(",")
          LoginEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
        }
      )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: LoginEvent): Long = element.timestamp * 1000L
      })

    // TODO: 复杂时间处理 乱序数据 复杂逻辑
    // TODO: 5秒内,连续三次登陆失败,报警
    //1. 定义cep匹配模式,需求:一个登陆失败时间后,紧跟另一个失败时间
    val loginFailPattern = Pattern
      .begin[LoginEvent]("firstFail").where(_.eventType == "fail")
      .next("secondFail").where(_.eventType == "fail")
      .next("thirdFail").where(_.eventType == "fail")
      .within(Time.seconds(5))

    //2.将数据流进行模式匹配 获取PatternStream
    val patternStream = CEP.pattern(loginEventStream.keyBy(_.userId), loginFailPattern)

    //3.调用select方法,筛选出匹配数据流
    val loginFailWarningStream = patternStream.select(new LoginFailEventMatch())

    loginFailWarningStream.print()
    env.execute("login fail detect with cep job")

  }
}

// TODO:  实现 PatternSelectFunction selct(...)
class LoginFailEventMatch() extends PatternSelectFunction[LoginEvent,LoginFailWarning]{
  override def select(map: util.Map[String, util.List[LoginEvent]]): LoginFailWarning = {
    // 匹配到的时间序列,保存在map
    val firstFailEvent = map.get("firstFail").get(0)
    val thirdFailEvent = map.get("thirdFail").iterator().next()
    LoginFailWarning(firstFailEvent.userId,firstFailEvent.timestamp,thirdFailEvent.timestamp,"login fail")
  }
}