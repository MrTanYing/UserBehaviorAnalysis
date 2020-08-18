package com.tlh.login.detect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @Comment 登陆失败检测漏洞版 不考虑乱序数据 不考虑2秒内大于两次登录失败,有登录成功时,导致状态清空
 * @Author: tlh
 * @Date 2020/8/17 16:20
 * @Version 1.0
 */
object LoginFail {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度,根据环境决定,设置为1便于测试
    env.setParallelism(1)
    //设置事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //数据源
    val resource = getClass.getResource("/LoginLog.csv")
    val inputStream = env.readTextFile(resource.getPath)

    //转换样例类,提取时间戳,设置watermark
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

    //需求:连续2秒,登陆失败就报警 实现逻辑
    val loginFailWarningStream = loginEventStream
      .keyBy(_.userId)
      .process(new LoginFailWarningResult(2))

    loginFailWarningStream.print()
    env.execute("login fail detect job")

  }
}

//输入的登录时间样例类 同一包下,样例类不能重复定义
case class LoginEvent(userId: Long, ip:String, eventType: String, timestamp: Long)
//输出报警信息样例类
case class LoginFailWarning(userId: Long, firstFailTime: Long, LastFailTime: Long, warnMsg: String)

class LoginFailWarningResult(failTimes: Int) extends KeyedProcessFunction[Long,LoginEvent,LoginFailWarning]{
  //需要保存当前登陆失败事件,保存定时器时间戳
  lazy val loginFailListState: ListState[LoginEvent] = getRuntimeContext.getListState(
    new ListStateDescriptor[LoginEvent]("loginfail-list", classOf[LoginEvent]))

  lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(
    new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, 
    LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {
    if (value.eventType == "fail"){
      loginFailListState.add(value)
      //如果没有定时器,注册一个两秒后的定时器
      if(timerTsState.value() == 0L){
        val ts = value.timestamp * 1000L + 2000L
        ctx.timerService().registerEventTimeTimer(ts)
        timerTsState.update(ts)
      }
    } else {
      //如果登录成功,清空状态和定时器
      ctx.timerService().deleteEventTimeTimer(timerTsState.value())
      loginFailListState.clear()
      timerTsState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent,
    LoginFailWarning]#OnTimerContext, out: Collector[LoginFailWarning]): Unit = {
    val allLoginFailList = new ListBuffer[LoginEvent]()
    val iter = loginFailListState.get().iterator()
    while(iter.hasNext){
      allLoginFailList += iter.next()
    }
    //2秒内连续登录失败2次就报警
    if(allLoginFailList.length > failTimes){
      out.collect(
        LoginFailWarning(
          allLoginFailList.head.userId,
          allLoginFailList.head.timestamp,
          allLoginFailList.last.timestamp,
          "login fail in 2s for " + allLoginFailList.length + " times."
        )
      )
    }
  }
}