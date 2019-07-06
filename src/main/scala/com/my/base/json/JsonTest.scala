package com.my.base.json

import com.alibaba.fastjson.JSON


object JsonTest {
  def main(args: Array[String]): Unit = {
    val str = """{"word": "无锡_0.68769437213,车友_0.453330347378,汽车_0.310452441637,网名_0.226539550569,雷斯特_0.127691904249", "label": "auto"}"""
    val mes = JSON.parseObject(str, classOf[LogGen])
    print(mes.getWord)
  }

}
