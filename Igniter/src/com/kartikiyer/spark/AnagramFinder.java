package com.kartikiyer.spark;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;


public class AnagramFinder extends PreConfigured implements Serializable
{
	public static void findAnagrams()
	{
		JavaRDD<String> inputFile = sc.textFile("Anagrams.txt");
		JavaPairRDD<Integer, String> mapOutput = inputFile.mapToPair(word -> new Tuple2<Integer, String>(word.length(), word));

		JavaPairRDD<Integer, List<String>> reducerInput = mapOutput.aggregateByKey(new ArrayList<>(), (v1, v2) ->
		{
			v1.add(v2);
			return v1;
		}, (v1, v2) ->
		{
			v1.addAll(v2);
			return v1;
		});

		JavaRDD<JavaPairRDD<String, String>> cartesianProdOfStrings = reducerInput.mapValues(x -> sc.parallelize(x).cartesian(sc.parallelize(x))).map(t -> t._2);

		/// create a tuple <str,sortedStr>
		// reduce on sorted string.. ::

		cartesianProdOfStrings.foreach(setOfBigrams ->
		{
			setOfBigrams.filter(t -> t._1.equals(t._2) == false).filter(t ->
			{
				char[] wordOne = t._1.toCharArray();
				char[] wordTwo = t._2.toCharArray();
				Arrays.sort(wordOne);
				Arrays.sort(wordTwo);
				return Arrays.equals(wordOne, wordTwo);
			}).foreach(f -> System.out.println(f));
		});


	}

	public static void main(String[] args)
	{
		AnagramFinder.findAnagrams();
	}
}

