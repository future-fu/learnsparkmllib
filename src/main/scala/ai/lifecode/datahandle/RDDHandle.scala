package ai.lifecode.datahandle

import ai.lifecode.utils.ShowRddDetails
import org.apache.spark.sql.SparkSession

/**
  * Created by future_fu on 2018/8/31 16:05.
  */
object RDDHandle {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .master("local[3]")
      .appName("rdd handle")
      .getOrCreate()
    val sc=spark.sparkContext
    //zip 1对1合并操作
    val zipRdd1=sc.parallelize(Array(1,2,3,4),3)
    val zipRdd2=sc.parallelize(Array("a","b","c","d"),3)
    val zipRdd=zipRdd1.zip(zipRdd2)
    println()
   ShowRddDetails.showRDD(zipRdd,"zipRdd",true)


    //subtract减法操作
    val subRdd1=sc.parallelize(1 to 9,3)
    val subRdd2=sc.parallelize(1 to 3,3)
    val subRdd=subRdd1.subtract(subRdd2)
    ShowRddDetails.showRDD(subRdd,"subRdd",true)

    //randomSplit
    val randomSplitRdd1=sc.parallelize(1 to 9 ,3)
    val randomSplitRdd=randomSplitRdd1.randomSplit(Array(0.2,0.3,0.2,0.3))
    ShowRddDetails.showRDD(randomSplitRdd(0),"randomSplitRdd 0",true)
    ShowRddDetails.showRDD(randomSplitRdd(1),"randomSplitRdd 1",true)
    ShowRddDetails.showRDD(randomSplitRdd(2),"randomSplitRdd 2",true)
    ShowRddDetails.showRDD(randomSplitRdd(3),"randomSplitRdd 3",true)



  }

}
