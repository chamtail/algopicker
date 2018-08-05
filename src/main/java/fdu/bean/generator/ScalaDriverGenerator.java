package fdu.bean.generator;

import fdu.service.operation.operators.*;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Created by zhou on 2017/7/1.
 */
public class ScalaDriverGenerator implements OperatorVisitor {
    private String scalaProgram  = "";
    public static String masterIP;
	
    private final String importPackages = "import java.util.HashMap\n"
    		+ "import org.apache.spark.SparkConf \n"
    		+ "import org.apache.spark.streaming._ \n"
    		+ "import org.apache.spark.streaming.kafka._ \n"
			+ "import org.apache.spark.storage.StorageLevel \n"
//    	    + "@transient val ssc = new StreamingContext(sc, Seconds(1)) \n";
			+ "@transient val ssc = new StreamingContext(sc, Seconds(2)) \n";

	@Override
    public void visitDataSource(DataSource source) {

    	scalaProgram += "val zkQuorum = \"122.144.216.246:2181,122.144.216.237:2181,122.144.216.250:2181\"\n"
    			+ "val group = \"test\"\n"
    			+ "val topics = \"test-topic\"\n"
    			+ "val topicMap = topics.split(\",\").map((_, 1)).toMap \n"
//    			+ "val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2) \n";
				+ "val lines = ssc.socketTextStream(\"localhost\", 9998) \n";
		try {
			if (source.getName().equals("xc")) {
//				Runtime.getRuntime().exec("java -jar inputdata.jar");
			} else {
				Runtime.getRuntime().exec("./kafkaSource.sh test-topic " + source.getName() + ' ' + source.getFrequency());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

	@Override
	public void visitFilter(Filter filter) {
		// TODO Auto-generated method stub
//		scalaProgram += "val lines = lines.filter(" + filter.getName() + ") \n";
		scalaProgram += "val result = lines.map(x => x.split(\" \").length)\n";
//		scalaProgram += "lines.print()\n";
	}

	@Override
	public void visitWordCount(WordCount wordCount) {
//		// TODO Auto-generated method stub
		scalaProgram += "val words = lines.flatMap(_.split(\" \")) \n"
////				+ "val result = words.map(x => (x, 1L)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(10), Seconds(5)) \n";
////				+ "val result = words.map(x => (x, 1L)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(20), Seconds(10)) \n";
				+ "val result = words.map(x => (x, 1L)) \n";

//		scalaProgram += "val result = lines.map(x => (x, 1L)) \n";

//		scalaProgram += "result.print()\n";
		
//		val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
//		      val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
//		      val output = (word, sum)
//		      state.update(sum)
//		      output
//		    }
//		val stateDstream = words.map(x => (x, 1L)).mapWithState(
//			      StateSpec.function(mappingFunc).initialState(initialRDD))
		
//		val wordCounts.foreachRDD( rdd => {
//			import org.apache.spark.sql.hive.HiveContext
//			val sqlContext = new HiveContext(sc)
//			import hiveContext.implicits._
//			val df = rdd.map(x => (x._1,x._2,System.currentTimeMillis())).toDF("word", "count", "time")
//		    df.write.saveAsTable("table_name") }
//		)
		
		
//		import java.util.{Date, Properties}
//		val props = new Properties()
//		props.put("bootstrap.servers", "10.141.208.49:9092,10.141.208.47:9092,10.141.208.45:9092")
//		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//		val producer = new KafkaProducer[String, String](props)
//		wordcounts.foreachRDD(rdd => {
//			  rdd.foreachPartition(partitionOfRecords => {
//				  partitionOfRecords.foreach {
//					  val data = new ProducerRecord[String, String]("outputTopic", "")
//					  p.send(bytes)
//				  }
//			  }
	}
	
	@Override
	public void visitWindowAggregation(WindowAggregation windowAggregation) {
		// TODO Auto-generated method stub
		scalaProgram += "val numbers = lines.map(_.toDouble) \n"
				+ "val numbersAndCount = numbers.map((_, 1 , Double.MinValue, Double.MaxValue)) \n"
				+ "val aggregation = numbersAndCount.reduceByWindow({(t1, t2) => (t1._1 + t2._1, t1._2 + t2._2, t1._1 max t2._1 max t1._3 max t2._3, t1._1 min t2._1 min t1._4 min t2._4)}, Seconds(2), Seconds(2)) \n";
		switch (windowAggregation.getFunction()) {
			case "sum":
				scalaProgram += "val result = aggregation.map(_._1) \n";
			break;
			case "avg":
				scalaProgram += "val result = aggregation.map(x => x._1 / x._2) \n";
				break;
			case "max":
				scalaProgram += "val result = aggregation.map(_._3) \n";
				break;
			case "min":
				scalaProgram += "val result = aggregation.map(_._4) \n";
				break;
			default:
				break;
		}
		

	}
	
	@Override
	public void visitXC(XC xc) {
		// TODO Auto-generated method stub
		System.out.println("Enter in XC Node!");
		scalaProgram += "xcxcxc";
	}

	@Override
	public void visitOutput(Output output) {
//		 TODO Auto-generated method stub
		if (output.getDest().equals("HDFS")) {
			scalaProgram += "result.repartition(1).saveAsTextFiles(\"hdfs://" + masterIP + ":9000/user/hadoop/result_" + output.getDest() + "/r\") \n";
		} else if (output.getDest().equals("console")) {
			scalaProgram += "result.print() \n";
		} else if (output.getDest().equals("hive")) {
			scalaProgram += "import org.apache.spark.sql.hive.HiveContext\n" +
					"val sqlContext = new HiveContext(sc)\n" +
					"import sqlContext.implicits._\n" +
					"import scala.util.Random\n" +
					"sqlContext.sql(\"DROP TABLE IF EXISTS sparktest.wa\")\n" +
					"sqlContext.sql(\"create table if not exists sparktest.wa (`tid` bigint, `value` int)\")\n" +
//					"result.map(x => (System.currentTimeMillis() / 1000, 1)).foreachRDD( rdd => {\n" +
					"result.foreachRDD( rdd => {\n" +
//		"val df = rdd.map(x => (x._1,System.currentTimeMillis())).toDF("agg", "time")
//					"val df = rdd.map(x => (System.currentTimeMillis() / 1000, 1)).toDF(\"tid\", \"values\")\n" +
//					"val df = rdd.toDF(\"tid\", \"values\")\n" +
					"val time = System.currentTimeMillis() / 2000 * 2000\n" +
					"val count = rdd.count()\n" +
					"val df = rdd.map(x => (time, count)).toDF(\"tid\", \"values\")\n" +
					"df.limit(1).write.insertInto(\"sparktest.wa\")\n" +
					"df.show(1)" +
					"}\n)\n";
//	)
		}
	}
	
    @Override
    public String generate(String id) {
        return importPackages 
//        		+ "ssc.checkpoint(\"hdfs://" + masterIP + ":9000/user/hadoop/checkpoint_" + id + "\")\n" //checkpoint must be a hdfs dir
				+ "ssc.checkpoint(\"hdfs://" + "10.141.208.43" + ":9000/user/hadoop/checkpoint_" + id + "\")\n" //checkpoint must be a hdfs dir
        		+ scalaProgram
        		+ "ssc.start() \n";
//        		+ "ssc.awaitTerminationOrTimeout(1000*20) \n"
//        		+ "println(\"end......\") \n";
    }
}
