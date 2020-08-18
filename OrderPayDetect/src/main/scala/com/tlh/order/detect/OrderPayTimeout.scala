package com.tlh.order.detect

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


  }

}
