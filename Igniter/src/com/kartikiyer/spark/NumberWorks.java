package com.kartikiyer.spark;


import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;


public class NumberWorks extends PreConfigured
{
	private void doCoolStuff()
	{
		JavaRDD<Integer> intRdd = sc.parallelize(Arrays.asList(new Integer[] { 1, 2, 3, 4, 5 }));
		JavaRDD<Integer> sqrdIntRdd = intRdd.map(x -> x * x);

		sqrdIntRdd.reduce((x, y) -> x + y);

		sqrdIntRdd.mapToPair(t -> null);

		System.out.println(sqrdIntRdd.reduce((x, y) -> x + y));
	
	}
	
	private void findAvg()
	{

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
		
		System.out.println(output.collect().toString());

	}
	
	public static void main(String[] args)
	{
		new NumberWorks().findAvg();
	}
}
