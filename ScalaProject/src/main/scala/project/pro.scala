package project

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import sys.process._

object pro {
  
  
  
   def main(args:Array[String]): Unit={
  
  val conf = new SparkConf() .setAppName("data") .setMaster("local[*]")
       val sc = new SparkContext(conf)
         sc.setLogLevel("Error")
         
         
         val spark = SparkSession.builder()
        
         .getOrCreate
         
           import spark.implicits._
           
       println("================Reading Sample Avro Data ======================================")    

           val df = spark.read.format("com.databricks.spark.avro").load("file:///C:/data/complex/projectsample.avro")
                                         df.show()
                                         //df.printSchema()
                                         
         println
         println
                                         
      println("================Reading Web api Data ======================================")     
      
                        val data = scala.io.Source
                  				.fromURL("https://randomuser.me/api/0.8/?results=10")
                  				.mkString
                  				val rdd =  sc.parallelize(List(data))
                  				val df1 = spark.read.json(rdd)
                  				
                  				     //df1.show()
                  				     //df1.printSchema()
                  				     
                                         
      println
      println
                                         
      println("================Flattening the Web api Data ======================================")   
  
         val flatdf = df1.withColumn("results", explode(col("results")))
                //flatdf.show()
                //flatdf.printSchema()
                
                
       println
      println
                                         
      println("================Result of Flattening the Web api Data ======================================")   
  
         val flatdfres = flatdf.select(
         
                     "nationality","seed","version",
    "results.user.username","results.user.cell","results.user.dob","results.user.email",
    "results.user.gender","results.user.location.city","results.user.location.state",
    "results.user.location.street","results.user.location.zip","results.user.md5",
    "results.user.name.first","results.user.name.last","results.user.name.title",
    "results.user.password","results.user.phone","results.user.picture.large","results.user.picture.medium"
    ,"results.user.picture.thumbnail","results.user.registered","results.user.salt","results.user.sha1"
    ,"results.user.sha256"
                     
                
             
         
         
         
         )
                //flatdfres.show()
                //flatdfres.printSchema()   
                
  println("================Removing numericals from username in the above dataframe ======================================")
   
    val rem = flatdfres.withColumn("username",regexp_replace(col("username"),  "([0-9])", ""))
					          //  rem.show()
		
    
    
  println("====================== Step 6 =========Joined Dataframe=============================================")

				val joindf = df.join(broadcast(rem),Seq("username"),"left")


				joindf.show()


				println("=================== Step 7 a ============Not available customers=============================================")



				val dfnull = joindf.filter(col("nationality").isNull)



				dfnull.show()



				println("==================  Step 7 b =============available customers=============================================")



				val dfnotnull=joindf.filter(col("nationality").isNotNull)

				
				dfnotnull.show()  
				
				println("=============== Step 8 ================Null handled dataframe=============================================")



				val replacenull= dfnull.na.fill("Not Available").na.fill(0)
				replacenull.show()

println("=============== Step 9 a ================not available customers with current date dataframe=============================================")




				val replacenull_with_current_date=replacenull.withColumn("current_date",current_date)
				.withColumn("current_date",col("current_date").cast(StringType))

				replacenull_with_current_date.show()


				println("=============== Step 9 b ================available customers with current date dataframe=============================================")




				val notnull_with_current_date=dfnotnull.withColumn("current_date",current_date)
				.withColumn("current_date",col("current_date").cast(StringType))


				notnull_with_current_date.show()

				//notnull_with_current_date.printSchema()

    
    
	 /*  				            
println("================Brodcast Left Join ======================================")		

    val joindf = df.join(broadcast(rem),Seq("username"),"left")
   
                joindf.show()
             
println("===================  ============Not available customers=============================================")
				

					val dfnull = joindf.filter(col("nationality").isNull)
dfnull.show()   


			



					       
println("=================== ============ available customers=============================================")
				

					


					val dfnotnull=joindf.filter(col("nationality").isNotNull)

			



					dfnotnull.show()   
					*        					
					*/
              
}
   
   
   
  
}