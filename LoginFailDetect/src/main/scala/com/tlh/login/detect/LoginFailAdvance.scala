package com.tlh.login.detect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @Comment 登陆失败检测改进版 不考虑乱序数据
 * @Author: tlh
 * @Date 2020/8/17 16:20
 * @Version 1.0
 */
object LoginFailAdvance {
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

    //需求:连续2秒,登陆失败就报警 实现KeyedProcessFunction
    val loginFailWarningStream = loginEventStream
      .keyBy(_.userId)
      .process(new LoginFailWarningAdvanceResult(2))
    loginFailWarningStream.print()
    env.execute("login fail detect Advance job")

  }
}

class LoginFailWarningAdvanceResult(failTimes: Int) extends KeyedProcessFunction[Long,LoginEvent,LoginFailWarning]{
  //需要保存当前登陆失败事件,保存定时器时间戳
  lazy val loginFailListState: ListState[LoginEvent] = getRuntimeContext.getListState(
    new ListStateDescriptor[LoginEvent]("loginfail-list", classOf[LoginEvent]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, 
    LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {
    //事件类型

    if (value.eventType == "fail"){
      //1.失败
      val iter = loginFailListState.get().iterator()
      //1.1判断是否有登陆失败事件
      if(iter.hasNext){
        //1.1.1 判断两次事件的事间差
        val firstLoginFailEvent = iter.next()
        if(value.timestamp < firstLoginFailEvent.timestamp + 2){
          //两秒内输出报警
          out.collect(
            LoginFailWarning(
              value.userId,
              firstLoginFailEvent.timestamp,
              value.timestamp,
              "login fail 2 times in 2s"
            )
          )
        }
        //1.1.2 当前数据处理完毕,状态清空后更新
        loginFailListState.clear()
        loginFailListState.add(value)
      } else {
        //1.2 添加状态
        loginFailListState.add(value)
      }
    } else {
      //2.如果登录成功,清空状态和定时器
      loginFailListState.clear()
    }
  }

}