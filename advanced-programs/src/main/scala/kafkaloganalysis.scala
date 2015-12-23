import kafka.serializer.StringDecoder
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._


object LogAnalysis {
  
//Write general vals and logic outside the main function contatining the producer

  //params for initializing kafka stream

    val Array(zkQuorum, group, topics, numThreads) = args
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val kafkaParams = Map[String, String]("metadata.broker.list" -> optionArgs(2).getOrElse("sandbox:6667"))
    


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("hw6")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(100)) //Specify the stream pickup time 

    ssc.checkpoint("checkpoint")  //In order to use updateByKey

// READ THE LOGS

    val listen = new """"PRODUCER""""


    val messages = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER).map(_._2)
    val logLinesDStream = streamingContext.socketTextStream("localhost", 9999)

    val accessLogsDStream = logLinesDStream.map(ApacheAccessLog.parseLogLine).cache()

    val listen = new Job_Listener(ssc)
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val table_zero = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER).map(_._2)
    val windowed_table_zero = table_zero.window(new Duration(100000), new Duration(100000))


//=======KAFKA========

//Setting up Kafka Stream   

    

    //creating a kafka dstream 
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)


//

// Set up the input DStream to read from Kafka (in parallel)
    val kafkaLogsStream = {
      
      val sparkStreamingConsumerGroup = "spark-streaming-consumer-group"
      val kafkaParams = Map(
        "zookeeper.connect" -> "zookeeper1:2181",
        "group.id" -> "spark-streaming-test",
        "zookeeper.connection.timeout.ms" -> "1000")
      
      val inputTopic = "input-topic"
      val numPartitionsOfInputTopic = 5
      
      val streams = (1 to numPartitionsOfInputTopic) map { _ =>
        KafkaUtils.createStream(ssc, kafkaParams, Map(inputTopic -> 1), StorageLevel.MEMORY_ONLY_SER).map(_._2)
      }

      val unifiedStream = ssc.union(streams)

      val sparkProcessingParallelism = 1 // You'd probably pick a higher value than 1 in production.
      unifiedStream.repartition(sparkProcessingParallelism)
    }

// We use accumulators to track global "counters" across the tasks of our streaming app
    val numInputMessages = ssc.sparkContext.accumulator(0L, "Kafka messages consumed")
    val numOutputMessages = ssc.sparkContext.accumulator(0L, "Kafka messages produced")
    // We use a broadcast variable to share a pool of Kafka producers, which we use to write data from Spark to Kafka.
    val producerPool = {
      val pool = createKafkaProducerPool(kafkaZkCluster.kafka.brokerList, outputTopic.name)
      ssc.sparkContext.broadcast(pool)
    }


//Application Logic



    // Define the actual data flow of the streaming job
      
    val sparkLogsStream = ssc.textFileStream("/Users/farrukhjadoon/Desktop/hw6/spark-logs-directory")

    sparkLogsStream.foreachRDD { rdd => {
      val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
      
      import sqlContext.implicits._

      rdd.toDF().registerTempTable("dfdsf")











    LogAnalyzerTotal logAnalyzerTotal = new LogAnalyzerTotal();
    LogAnalyzerWindowed logAnalyzerWindowed = new LogAnalyzerWindowed();

    // Process the DStream which gathers stats for all of time.
    logAnalyzerTotal.processAccessLogs(accessLogsDStream);

    // Calculate statistics for the last time interval.
    logAnalyzerWindowed.processAccessLogs(accessLogsDStream);

    // Render the output each time there is a new RDD in the accessLogsDStream.
    Renderer renderer = new Renderer();
    accessLogsDStream.foreachRDD(rdd -> {
      // Call this to output the stats.
      renderer.render(logAnalyzerTotal.getLogStatistics(),
          logAnalyzerWindowed.getLogStatistics());
      return null;
    });




    val filtered = messages.filter { case (key, message) => {
      regex.findFirstIn(message) match {
        case Some(m) => true
        case None => false
      }
    }
    }

    
    val commands = filtered.map { case (key, message) => {
      val matched = regex.findFirstMatchIn(message).get
      Audit(matched.group(1), matched.group(2),
        matched.group(3), matched.group(4))
    }
    }

    commands.foreachRDD { (rdd, time) => {
      val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)

      import sqlContext.implicits._

      rdd.toDF().registerTempTable("commands")

      println(s"========= $time =========")

      sqlContext.sql("select command, count(*) as invocation_count from commands group by command").show()
    }
    }

//Must use these commands

    ssc.start()
    ssc.awaitTermination()
  }
}