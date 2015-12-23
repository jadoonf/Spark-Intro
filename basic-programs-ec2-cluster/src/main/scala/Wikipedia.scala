/* Wikipedia.scala */
/* For running on cluster */


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext._


object Wikipedia {


  val MASTER = "[master_public_dns]" // Can be changed to the ip address too
  val SPARK_MASTER = "spark://" + MASTER + ":7077"
  val HDFS = "hdfs://" + MASTER + ":9000"

  //change the following paths for your own cluster accordingly

  val outputDirNoOutlinks = HDFS + "/Wikipedia-output/no-outlinks" 
  val outputDirNoInlinks = HDFS + "/Wikipedia-output/no-inlinks"
 
  def main(args: Array[String]) {
    
    
    	val conf = new SparkConf()
    	.setMaster(MASTER)  
      .setAppName("basic")  
	

		  val sc = new SparkContext(conf)
    	val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        

  		val wikiData = sc
  		.textFile("s3n://[access_id:secret_key]@spark-farrukh/links-simple-sorted.txt") //change to your respective s3 bucket path 

  		//specifying the dataframes schema progmatically

      val schemaString = "page_id outlinks_page_id"   

  		import org.apache.spark.sql.Row;   
  		import org.apache.spark.sql.types.{StructType,StructField,StringType};

  		val schema =
  	  		StructType(
        		schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true))) 

  		val wikiDataRDD = wikiData.map(_.split(":")).map(l => Row(l(0), l(1).trim)) 
  	
		  val wikiDataFrame = sqlContext.createDataFrame(wikiDataRDD, schema) 

      //application logic

  		wikiDataFrame.registerTempTable("row1_and_row2_table")

    	val lhsQuery = sqlContext
    		.sql("SELECT page_id FROM row1_and_row2_table")
        .distinct 
        lhsQuery.explain(true)

   	 	val rhsQuery = sqlContext
    		.sql("SELECT outlinks_page_id FROM row1_and_row2_table")

        rhsQuery.explain(true)
		
		import sqlContext.implicits._ //for .toDF() conversion 

		val rhsToString = rhsQuery
      
			.map(row => row.mkString(""))
  			.flatMap(_.split("\\s+"))
  			.toDF()
  			.distinct

		val titlesData = sc.textFile("s3n://access_id:secret_key@spark-farrukh/titles-sorted.txt")

    val titlesDataFrame = titlesData.zipWithIndex.map{case(k,v) =>(v,k)}.toDF() 

      	titlesDataFrame.registerTempTable("titles")

    val indices = sqlContext.sql("SELECT _1 FROM titles")

    indices.explain(true) 

		val NoOutlinksRDD = indices.except(lhsQuery).rdd   
      	.saveAsTextFile(outputDirNoOutlinks)
     
    val NoInlinksRDD = indices.except(rhsToString).rdd 
      	.saveAsTextFile(outputDirNoInlinks)
	}
}