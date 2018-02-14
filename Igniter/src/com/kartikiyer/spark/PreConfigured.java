package com.kartikiyer.spark;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class PreConfigured
{
	static SparkConf conf;
	static JavaSparkContext sc;
	
	static {
		LogManager.getLogger("org").setLevel(Level.WARN);
		System.setProperty("hadoop.home.dir", "C:\\winutils-master\\hadoop-2.7.1");
		conf = new SparkConf().setMaster("local[4]").setAppName("mark1");
		sc = new JavaSparkContext(conf);
	}
}
