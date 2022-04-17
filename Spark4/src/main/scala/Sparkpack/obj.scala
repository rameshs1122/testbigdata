package Sparkpack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession 
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.mysql.jdbc._
import sys.process._

object obj {

  def main(args:Array[String]): Unit={
  
  val conf = new SparkConf() .setAppName("data") .setMaster("local[*]")
       val sc = new SparkContext(conf)
         sc.setLogLevel("Error")
         
         
         val spark = SparkSession.builder()
        
         .getOrCreate
         
           import spark.implicits._

        val df = spark.read.format("json").option("multiLine","true")
                .load("file:///C://data/complex//complex2.json")
                
                df.show()
                
                df.printSchema()
                
    
     
     println("falttening the data farame")
     
                 val flatdf= df.select(
                     
                            "address.*",
                            "orgname",
                            "trainer",
                            "user.*"
                 
                 )
           
                    flatdf.show()
                    flatdf.printSchema()
                    
                    
      println("exploding  the data farame")  
     
                       val expdf = flatdf.withColumn("Students",explode(col("Students")))
                                expdf.show()
                                expdf.printSchema()
                               
                    
                    
                    
         println("===============Struct complex data createfrmae===================================")  
         
                    val strcutdf = expdf.select(
                    
                          col("Students"),
                          struct(
                              
                                col("permanent_address"),col("temporary_address")
                          
                          
                          
                          ).alias("address"),
                          
                          col("orgname"),
                          col("trainer")
                    
                    )
         
                              strcutdf.show()
                              strcutdf.printSchema()
         
         
                  
     println("===============Array aggregation complex data createfrmae===================================")
     
                val arradf = strcutdf.groupBy("address","orgname","trainer")
                             .agg(collect_list("Students").alias("Students"))
                             
                             
                             arradf.show()
                             arradf.printSchema()
     
     
     
     
    
}
  

  
  
}