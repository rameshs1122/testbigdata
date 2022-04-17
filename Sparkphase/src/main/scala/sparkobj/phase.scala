package sparkobj

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession 
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import sys.process._

object obj {

  def main(args:Array[String]): Unit={
  
  val conf = new SparkConf() .setAppName("data") .setMaster("local[*]")
       val sc = new SparkContext(conf)
         sc.setLogLevel("Error")
         
         
         val spark = SparkSession.builder()
        
         .getOrCreate
         
           import spark.implicits._
           
       println("================Reading XML Data ======================================")    

           val df = spark.read.format("com.databricks.spark.xml").option("rowTag","POSLog").load("file:///C:/data/complex/transactions.xml")
                                         df.show()
                                         df.printSchema()
      
}
  

  
  
}