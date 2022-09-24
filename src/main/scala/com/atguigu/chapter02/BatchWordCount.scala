package com.atguigu.chapter02

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment, createTypeInformation}

object BatchWordCount {

  def main(args : Array[String]):Unit= {
    //TODO 创建执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //TODO 读取文本文件
    val lineDS: DataSet[String] = env.readTextFile("D:\\WorkSpace_IDEA\\flink-scala\\input\\words.txt")

    //TODO 对数据进行格式转换
    val wordToOne: DataSet[(String, Int)] = lineDS.flatMap(_.split(" ")).map((_, 1))
    val result: AggregateDataSet[(String, Int)] = wordToOne.groupBy(0).sum(1)
    result.print();

  }
}
