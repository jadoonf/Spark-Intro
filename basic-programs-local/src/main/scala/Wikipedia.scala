/* Wikipedia.scala */
/* For local */


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext._


object Wikipedia {

	val outputDirNoOutlinks = "/home/ubuntu/hw1/Wikipedia-output/no-outlinks"
 	val outputDirNoInlinks = "/home/ubuntu/hw1/Wikipedia-output/no-inlinks"
  	
  
  def main(args: Array[String]) {
    
    
    	val conf = new SparkConf()
    	.setAppName("hw1")	
		.setMaster("local[*]") 
	

		val sc = new SparkContext(conf)
    	val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        

  		val wikiData = sc
  		.textFile("/home/ubuntu/hw1/links-simple-sorted.txt") //change for ubuntu

  	


  		val schemaString = "page_id outlinks_page_id" 


  		import org.apache.spark.sql.Row;   
  		import org.apache.spark.sql.types.{StructType,StructField,StringType};



  		val schema =
  	  		StructType(
        		schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true))) 


  		val wikiDataRDD = wikiData.map(_.split(":")).map(l => Row(l(0), l(1).trim)) 
  	


		val wikiDataFrame = sqlContext.createDataFrame(wikiDataRDD, schema) 

  	
  		wikiDataFrame.registerTempTable("row1_and_row2_table")




    	val lhsQuery = sqlContext
    		.sql("SELECT page_id FROM row1_and_row2_table")
    		.distinct  

	
			


   	 	val rhsQuery = sqlContext
    		.sql("SELECT outlinks_page_id FROM row1_and_row2_table")
		
		import sqlContext.implicits._


		

		val rhsToString = rhsQuery
      
			.map(row => row.mkString(""))
  			.flatMap(_.split("\\s+"))
  			.toDF()
  			.distinct

		

		val titlesData = sc.textFile("/home/ubuntu/hw1/titles-sorted.txt")




     	val titlesDataFrame = titlesData.zipWithIndex.map{case(k,v) =>(v,k)}.toDF() 

      	titlesDataFrame.registerTempTable("titles")

      	val indices = sqlContext.sql("SELECT _1 FROM titles")

	
	
  

		val NoOutlinksRDD = indices.except(lhsQuery).rdd   
      	.saveAsTextFile(outputDirNoOutlinks)
     
       
      	val NoInlinksRDD = indices.except(rhsToString).rdd 
      	.saveAsTextFile(outputDirNoInlinks)



	}
}