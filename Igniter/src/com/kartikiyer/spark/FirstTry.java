package com.kartikiyer.spark;

import java.util.ArrayList;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

public class FirstTry extends PreConfigured {

	public static void main(String[] args) {

		Logger.getLogger("org").setLevel(Level.OFF);
		System.setProperty("hadoop.home.dir", "G:\\HadoopTraining\\winutils-master\\winutils-master\\hadoop-2.7.1");


		JavaRDD<String> input = sc.textFile("C:/Users/kartik.iyer/Desktop/Weather.txt");

		System.out.println(input.count());

		ArrayList<String> hs = new ArrayList<String>();

		JavaPairRDD<String, ArrayList<String>> reducerInput = input
				.mapToPair(line -> new Tuple2<String, String>(line.split("\\t")[0].split("-")[2], line.split("\\t")[1]))
				.aggregateByKey(hs, (c, e) -> {
					c.add(e);
					return c;
				}, (hs1, hs2) -> {
					hs1.addAll(hs2);
					return hs1;
				});

		System.out.println(reducerInput.collect().toString());

		JavaPairRDD<String, String> output = reducerInput.mapValues(
				s -> sc.parallelize(s).reduce((a, b) -> String.valueOf(
						(Integer.parseInt(a) + Integer.parseInt(b))/s.size()
						)));
		
		System.out.println(reducerInput.collect().toString());
	}

}
