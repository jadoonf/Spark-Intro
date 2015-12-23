/* WhiteHouse.scala */
/* For cluster */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext._
//import com.databricks.spark.csv
//import com.databricks.spark.csv._

object WhiteHouse {

	val MASTER = "ec2-52-23-200-195.compute-1.amazonaws.com" // changing to ip / public dns might work
  	val SPARK_MASTER = "spark://" + MASTER + ":7077"
  	val HDFS = "hdfs://" + MASTER + ":9000"

	val visitorsDir = HDFS +"/Whitehouse-output/visitors" //change path for cluster and local accordingly 
	val visiteesDir = HDFS +"/Whitehouse-output/visitees"
	val combinationDir = HDFS +"/Whitehouse-output/combination"

	def main(args: Array[String]) {
		
		val conf = new SparkConf()
		.setMaster("spark://ec2-52-23-200-195.compute-1.amazonaws.com:7077")   
      	.setAppName("hw5")  

		val sc = new SparkContext(conf)

		val sqlContext = new SQLContext(sc) 
		
		

		val df = sqlContext
		.read
		.format("com.databricks.spark.csv") 
		.option("header", "true")
		.load("s3n://access_id:secret_key@spark-farrukh/whitehouse_waves-2015_09.csv") //change path for cluster and local accordingly 
		

//Create table for SQL querying


		val dfTable = df.toDF()

		dfTable.registerTempTable("whitehouse_table")


//Most frequent vistors

		

		val vistorsQuery = sqlContext
		.sql("SELECT NAMELAST, NAMEFIRST, NAMEMID FROM whitehouse_table")

		vistorsQuery.explain(true)

		val projectVisitorsQuery = vistorsQuery.map( name => ( name, 1 ))      ////as (k,v) 

		val reduceVisitorsQuery = projectVisitorsQuery.reduceByKey(_+_)

		val sortVisitorsQuery = reduceVisitorsQuery
		
		.takeOrdered(10)(Ordering[Int].reverse.on(x => x._2))


		val visitorsRDD = sc.parallelize(sortVisitorsQuery)
		.saveAsTextFile(visitorsDir)

//Most frequent visitees
		
	

		val visiteesQuery = sqlContext
		.sql("SELECT visitee_namefirst, visitee_namelast FROM whitehouse_table")

		visiteesQuery.explain(true)

		val projectVisiteesQuery = visiteesQuery.map( name => ( name, 1 ))      ////as (k,v) 

		val reduceVisiteesQuery = projectVisiteesQuery.reduceByKey(_+_)

		val sortVisiteesQuery = reduceVisiteesQuery
		
		.takeOrdered(10)(Ordering[Int].reverse.on(x => x._2))

		val visiteesRDD = sc.parallelize(sortVisiteesQuery)
		.saveAsTextFile(visiteesDir)


//Most frequent combination


		val combinationQuery = sqlContext
		.sql("SELECT NAMELAST, NAMEFIRST, NAMEMID, visitee_namefirst, visitee_namelast FROM whitehouse_table")

		combinationQuery.explain(true)

		val projectCombinationQuery = combinationQuery.map( name => ( name, 1 ))      ////as (k,v) 

		val reduceCombinationQuery = projectCombinationQuery.reduceByKey(_+_)


		
		val sortCombinationQuery = reduceCombinationQuery.takeOrdered(10)(new Ordering[(org.apache.spark.sql.Row, Int)] {
      	override def compare(x: (org.apache.spark.sql.Row, Int), y: (org.apache.spark.sql.Row, Int)) = { -x._2.compareTo(y._2) }
    	});

    	
		val combinationRDD = sc.parallelize(sortCombinationQuery)
		.saveAsTextFile(combinationDir)




	}
}