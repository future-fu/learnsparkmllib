package ai.lifecode.utils

import org.apache.spark.rdd.RDD

object ShowRddDetails {

  def  showRDD[T](rdd:RDD[T],info:String,globalDebugSwitch:Boolean): Unit ={
    val ti=this.getClass.toString
    val className=ti.filter( _ !='$')
    println(s"=== [${className}]: "+info+" ===")

    if(globalDebugSwitch) {
      val newRdd = rdd.map { a => {
        a match {
          case x: Array[_] => x.toList
          case _ => a
        }
       }
      }
      newRdd.collect().take(100).foreach(print)
      println
    }
  }



}
