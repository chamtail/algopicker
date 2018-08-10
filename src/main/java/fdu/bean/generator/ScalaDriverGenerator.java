package fdu.bean.generator;

import fdu.service.operation.operators.*;

import java.io.IOException;

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
            + "import java.util.concurrent.atomic.LongAdder \n"
            + "import org.apache.spark.sql.hive.HiveContext \n"
            + "import spark.implicits._\n"
			+ "@transient val ssc = new StreamingContext(sc, Seconds(2)) \n";

	@Override
    public void visitDataSource(DataSource source) {
//    	scalaProgram += "val lines = ssc.socketTextStream(\"localhost\", 9998)\n";
//    	scalaProgram += "var urlHitsMap:Map[String, LongAdder] = Map()\n";
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
        scalaProgram += "val lines = ssc.socketTextStream(\"localhost\", 9996)\n";
		scalaProgram += "val tuple = lines.map(x => x.split(\" \"))\n";
		scalaProgram += "val result = tuple.map(x => (x(2), 1L)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(4), Seconds(2))\n";
        scalaProgram += "val sqlContext = new HiveContext(sc)\n";
		scalaProgram += "sqlContext.sql(\"CREATE DATABASE IF NOT EXISTS sparktest\")\n";
        scalaProgram += "sqlContext.sql(\"DROP TABLE IF EXISTS sparktest.groupby\")\n";
        scalaProgram += "sqlContext.sql(\"CREATE TABLE IF NOT EXISTS sparktest.groupby (`tid` bigint, `url` varchar(100), `count` int)\")\n";
        scalaProgram += "val tid = System.currentTimeMillis() / 2000 * 2000\n";
        scalaProgram += "result.map(x => (System.currentTimeMillis() / 2000 * 2000, x._1, x._2)).foreachRDD( rdd => {\n" +
                "   val df = rdd.toDF(\"tid\", \"url\", \"count\")\n" +
                "   df.write.insertInto(\"sparktest.groupby\")\n" +
                "})\n";
	}
	
	@Override
	public void visitWindowAggregation(WindowAggregation windowAggregation) {
//		scalaProgram += "val numbers = lines.map(_.toDouble) \n"
//				+ "val numbersAndCount = numbers.map((_, 1 , Double.MinValue, Double.MaxValue)) \n"
//				+ "val aggregation = numbersAndCount.reduceByWindow({(t1, t2) => (t1._1 + t2._1, t1._2 + t2._2, t1._1 max t2._1 max t1._3 max t2._3, t1._1 min t2._1 min t1._4 min t2._4)}, Seconds(2), Seconds(2)) \n";
		switch (windowAggregation.getFunction()) {
			case "sum": {
                scalaProgram += "val lines = ssc.socketTextStream(\"localhost\", 9997)\n";
			    scalaProgram += "val sqlContext = new HiveContext(sc)\n";
				scalaProgram += "sqlContext.sql(\"CREATE DATABASE IF NOT EXISTS sparktest\")\n";
			    scalaProgram += "sqlContext.sql(\"DROP TABLE IF EXISTS sparktest.aggregation\")\n";
			    scalaProgram += "sqlContext.sql(\"CREATE TABLE IF NOT EXISTS sparktest.aggregation " +
                        "(`tid` bigint, `count` int)\")\n";
			    scalaProgram += "lines.foreachRDD( rdd => {\n" +
                        "   val time = System.currentTimeMillis() / 2000 * 2\n" +
                        "   val count = rdd.count()\n" +
                        "   val df = rdd.map(x => (time, count)).toDF(\"tid\", \"count\")\n" +
                        "   df.limit(1).write.insertInto(\"sparktest.aggregation\")\n" +
                        "   df.show(1)" +
                        "}\n)\n";
            }
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
//			scalaProgram += "import org.apache.spark.sql.hive.HiveContext\n" +
//					"val sqlContext = new HiveContext(sc)\n" +
//					"import sqlContext.implicits._\n" +
//					"sqlContext.sql(\"DROP TABLE IF EXISTS sparktest.wa\")\n" +
//					"sqlContext.sql(\"create table if not exists sparktest.wa (`tid` bigint, `value` int)\")\n" +
////					"result.map(x => (System.currentTimeMillis() / 1000, 1)).foreachRDD( rdd => {\n" +
//					"result.foreachRDD( rdd => {\n" +
////		"val df = rdd.map(x => (x._1,System.currentTimeMillis())).toDF("agg", "time")
////					"val df = rdd.map(x => (System.currentTimeMillis() / 1000, 1)).toDF(\"tid\", \"values\")\n" +
////					"val df = rdd.toDF(\"tid\", \"values\")\n" +
//					"val time = System.currentTimeMillis() / 2000 * 2000\n" +
//					"val count = rdd.count()\n" +
//					"val df = rdd.map(x => (time, count)).toDF(\"tid\", \"values\")\n" +
//					"df.limit(1).write.insertInto(\"sparktest.wa\")\n" +
//					"df.show(1)" +
//					"}\n)\n";
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
