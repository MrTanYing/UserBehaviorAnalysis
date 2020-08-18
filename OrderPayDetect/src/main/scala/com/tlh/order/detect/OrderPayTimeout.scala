package com.tlh.order.detect

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @Comment 订单支付超时检测 阈值15分钟
 * @Author: tlh
 * @Date 2020/8/18 19:38
 * @Version 1.0
 */
//输入样例类  txId 支付流水号
case class OrderEvent(orderId: Long, eventType: String, txId: String, timestamp: Long)
//输出样例类
case class OrderResult(orderId: Long, resultMsg: String)

object OrderPayTimeout {
  def main(args: Array[String]): Unit = {
    //0.环境初始化和数据源
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度,根据环境决定,设置为1便于测试
    env.setParallelism(1)
    //设置事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //数据源
    val resource = getClass.getResource("/OrderLog.csv")
    val inputStream = env.readTextFile(resource.getPath)

  }

}
