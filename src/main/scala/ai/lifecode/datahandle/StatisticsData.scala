package ai.lifecode.datahandle

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.SparkSession

/**
  * Created by future_fu on 2018/8/31 16:52.
  */
object StatisticsData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("rdd handle")
      .getOrCreate()
    val sc = spark.sparkContext
    val path = "data/sample_stat.txt"
    val data = sc.textFile(path)
      .map(_.split("\t"))
      .map(f => f.map(f => f.toDouble))
      .map(f => Vectors.dense(f))
    //计算每列最大值,最小值,平均值,方差值,L1范数,L2范数
    val stat1 = Statistics.colStats(data)
    println(stat1.max)
    println(stat1.min)
    println(stat1.mean)
    println(stat1.variance)
    println(stat1.normL1)
    println(stat1.normL2)

    //相关系数,取值范围[-1,1] 0不相关 [-1,0]负相关 [0,1]正相关
    val corr1=Statistics.corr(data,"pearson")
    val corr2=Statistics.corr(data,"spearman")
    val x1=sc.parallelize(Array(1.0,2.0,3.0,4.0))
    val x2=sc.parallelize(Array(5.0,6.0,6.0,6.0))
    val corr3=Statistics.corr(x1,x2,"pearson")

    println(corr1)
    println(corr2)
    println(corr3)


  }
}
