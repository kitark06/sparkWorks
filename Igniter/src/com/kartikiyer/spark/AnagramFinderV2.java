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


public class AnagramFinderV2 extends PreConfigured implements Serializable
{
	public String getSortedString(String word)
	{
		char[] charRepresentationOfStr = word.toCharArray();
		Arrays.sort(charRepresentationOfStr);
		return new String(charRepresentationOfStr);
	}

	public void findAnagrams()
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

		reducerInput.foreach(x ->
		{
			JavaPairRDD<String, ArrayList<String>> anagramRdd = sc	.parallelize(x._2)
														.mapToPair(s -> new Tuple2<String, String>(getSortedString(s), s))
														.aggregateByKey(new ArrayList<String>(), (anagramSets, word) ->
														{
															anagramSets.add(word);
															return anagramSets;
														}, (anagramSet1, anagramSet2) ->
														{
															anagramSet1.addAll(anagramSet2);
															return anagramSet1;
														});

			anagramRdd.filter(t -> t._2.size() > 1).foreach(t -> System.out.println(t._2.toString()));
		});

		reducerInput.sortByKey((Integer i1, Integer i2) -> -(i1 - i2));
	}

	public static void main(String[] args)
	{
		new AnagramFinderV2().findAnagrams();
	}
}

