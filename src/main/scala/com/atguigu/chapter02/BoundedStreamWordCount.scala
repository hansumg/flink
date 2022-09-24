package com.atguigu.chapter02

import org.apache.flink.streaming.api.scala._

object BoundedStreamWordCount {
  def main(args: Array[String]) : Unit ={

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val inputDS: DataStream[String] = env.readTextFile("input/words.txt")
    val wordToOne: DataStream[(String, Int)] = inputDS.flatMap(_.split(" ")).map((_, 1))

    val ruselt: DataStream[(String, Int)] = wordToOne.keyBy(_._1).sum(1)
    ruselt.print()
    env.execute()



  }

}
