/* WhiteHouse.scala */
/* For running on cluster */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext._

//import com.databricks.spark.csv
//import com.databricks.spark.csv._   //These libraries are currently being imported in the spark-submit script (see docs) 

object WhiteHouse {

	val MASTER = "[master_public_dns]" // Can be changed to the ip address too
  	val SPARK_MASTER = "spark://" + MASTER + ":7077"
  	val HDFS = "hdfs://" + MASTER + ":9000"

  	//change the following paths for your own cluster accordingly

	val visitorsDir = HDFS +"/Whitehouse-output/visitors" 
	val visiteesDir = HDFS +"/Whitehouse-output/visitees"
	val combinationDir = HDFS +"/Whitehouse-output/combination"

	def main(args: Array[String]) {
		
		val conf = new SparkConf()
		.setMaster(MASTER)   
      	.setAppName("basic")  

		val sc = new SparkContext(conf)

		val sqlContext = new SQLContext(sc) 

		val df = sqlContext
		.read
		.format("com.databricks.spark.csv") 
		.option("header", "true")
		.load("s3n://[access_id:secret_key]@spark-farrukh/whitehouse_waves-2015_09.csv") //change to your respective s3 bucket path 
		
		//Specifying the schema by inference

		val dfTable = df.toDF()

		dfTable.registerTempTable("whitehouse_table")

//application logic

//Most frequent vistors

		val vistorsQuery = sqlContext
		.sql("SELECT NAMELAST, NAMEFIRST, NAMEMID FROM whitehouse_table")

		vistorsQuery.explain(true) //prints schema information at runtime 

		val projectVisitorsQuery = vistorsQuery.map( name => ( name, 1 )) //as (k,v) pairs 

		val reduceVisitorsQuery = projectVisitorsQuery.reduceByKey(_+_)

		val sortVisitorsQuery = reduceVisitorsQuery
		
		.takeOrdered(10)(Ordering[Int].reverse.on(x => x._2))

		val visitorsRDD = sc.parallelize(sortVisitorsQuery)
		.saveAsTextFile(visitorsDir)

//Most frequent visitees

		val visiteesQuery = sqlContext
		.sql("SELECT visitee_namefirst, visitee_namelast FROM whitehouse_table")

		visiteesQuery.explain(true)

		val projectVisiteesQuery = visiteesQuery.map( name => ( name, 1 ))    //as (k,v) pairs

		val reduceVisiteesQuery = projectVisiteesQuery.reduceByKey(_+_)

		val sortVisiteesQuery = reduceVisiteesQuery
		
		.takeOrdered(10)(Ordering[Int].reverse.on(x => x._2))

		val visiteesRDD = sc.parallelize(sortVisiteesQuery)
		.saveAsTextFile(visiteesDir)

//Most frequent combination

		val combinationQuery = sqlContext
		.sql("SELECT NAMELAST, NAMEFIRST, NAMEMID, visitee_namefirst, visitee_namelast FROM whitehouse_table")

		combinationQuery.explain(true)

		val projectCombinationQuery = combinationQuery.map( name => ( name, 1 ))      //as (k,v) pairs 

		val reduceCombinationQuery = projectCombinationQuery.reduceByKey(_+_)
		
		val sortCombinationQuery = reduceCombinationQuery.takeOrdered(10)(new Ordering[(org.apache.spark.sql.Row, Int)] {
      	override def compare(x: (org.apache.spark.sql.Row, Int), y: (org.apache.spark.sql.Row, Int)) = { -x._2.compareTo(y._2) }
    	});    	
		val combinationRDD = sc.parallelize(sortCombinationQuery)
		.saveAsTextFile(combinationDir)

	}
}
