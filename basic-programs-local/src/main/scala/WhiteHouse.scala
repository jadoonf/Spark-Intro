/* WhiteHouse.scala */
/* For local */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext._
//import com.databricks.spark.csv
//import com.databricks.spark.csv._

object WhiteHouse {

	val visitorsDir = "/Users/farrukhjadoon/Desktop/Whitehouse-output/visitors" //change path for cluster and local accordingly 
	val visiteesDir = "/Users/farrukhjadoon/Desktop/Whitehouse-output/visitees"
	val combinationDir = "/Users/farrukhjadoon/Desktop/Whitehouse-output/combination"

	def main(args: Array[String]) {
		
		val conf = new SparkConf()
		.setAppName("hw1")	
		.setMaster("local[*]")  

		val sc = new SparkContext(conf)

		val sqlContext = new SQLContext(sc) 
		
		

		val df = sqlContext
		.read
		.format("com.databricks.spark.csv") 
		.option("header", "true")
		.load("whitehouse_waves-2015_11.csv") //change path for cluster and local accordingly 
		

//Create table for SQL querying


		val dfTable = df.toDF()

		dfTable.registerTempTable("whitehouse_table")


//Most frequent vistors

		

		val vistorsQuery = sqlContext
		.sql("SELECT NAMELAST, NAMEFIRST, NAMEMID FROM whitehouse_table")

		val projectVisitorsQuery = vistorsQuery.map( name => ( name, 1 ))      ////as (k,v) 

		val reduceVisitorsQuery = projectVisitorsQuery.reduceByKey(_+_)

		val sortVisitorsQuery = reduceVisitorsQuery
		
		.takeOrdered(10)(Ordering[Int].reverse.on(x => x._2))


		val visitorsRDD = sc.parallelize(sortVisitorsQuery)
		.saveAsTextFile(visitorsDir)

//Most frequent visitees
		
	

		val visiteesQuery = sqlContext
		.sql("SELECT visitee_namefirst, visitee_namelast FROM whitehouse_table")

		val projectVisiteesQuery = visiteesQuery.map( name => ( name, 1 ))      ////as (k,v) 

		val reduceVisiteesQuery = projectVisiteesQuery.reduceByKey(_+_)

		val sortVisiteesQuery = reduceVisiteesQuery
		
		.takeOrdered(10)(Ordering[Int].reverse.on(x => x._2))

		val visiteesRDD = sc.parallelize(sortVisiteesQuery)
		.saveAsTextFile(visiteesDir)


//Most frequent combination


		val combinationQuery = sqlContext
		.sql("SELECT NAMELAST, NAMEFIRST, NAMEMID, visitee_namefirst, visitee_namelast FROM whitehouse_table")

		val projectCombinationQuery = combinationQuery.map( name => ( name, 1 ))      ////as (k,v) 

		val reduceCombinationQuery = projectCombinationQuery.reduceByKey(_+_)


		
		val sortCombinationQuery = reduceCombinationQuery.takeOrdered(10)(new Ordering[(org.apache.spark.sql.Row, Int)] {
      	override def compare(x: (org.apache.spark.sql.Row, Int), y: (org.apache.spark.sql.Row, Int)) = { -x._2.compareTo(y._2) }
    	});

    	
		val combinationRDD = sc.parallelize(sortCombinationQuery)
		.saveAsTextFile(combinationDir)

		sc.stop()


	}
}