package com.kartikiyer.spark;


import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;


public class LogSifter extends PreConfigured
{
	private void siftLogs()
	{

		JavaRDD<String> inputRDD = sc.textFile("dblogs.txt");

		JavaPairRDD<String, Integer> levelCount = inputRDD.filter(line -> line.split(" ")[0].matches("\\d{4}-\\d{2}-\\d{2}"))
												.map(line -> line.split(" "))
												.mapToPair(lineSplits -> new Tuple2<String, Integer>(lineSplits[2], 1))
												.reduceByKey((v1, v2) -> v1 + v2)
												.mapToPair((row -> new Tuple2(row._1.toLowerCase(), row._2)));

		levelCount.filter(x -> x._1.length() > 1);

		for (String string : levelCount.collectAsMap().keySet())
			System.out.println(string + " --> " + levelCount.collectAsMap().get(string));

		// while (true){}
	}

	public static void main(String[] args)
	{
		new LogSifter().siftLogs();
	}
}
