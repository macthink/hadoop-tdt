package cn.macthink.hadoop.tdt.vectorization.wordlist;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import cn.macthink.hadoop.tdt.vectorizer.termlist.GenerateTermListMapper;
import cn.macthink.hadoop.tdt.vectorizer.termlist.GenerateTermListReducer;

public class GenerateWordListTest extends TestCase {

	// Mapper
	private Mapper<Text, Text, Text, IntWritable> mapper;
	private MapDriver<Text, Text, Text, IntWritable> mapDriver;

	// Reducer
	private Reducer<Text, IntWritable, Text, IntWritable> reducer;
	private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

	@Before
	public void setUp() throws Exception {
		mapper = new GenerateTermListMapper();
		mapDriver = new MapDriver<Text, Text, Text, IntWritable>(mapper);

		reducer = new GenerateTermListReducer();
		reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>(reducer);
	}

	@Test
	public final void testMap() {
		try {
			// @formatter:off
			mapDriver.withInput(new Text(), new Text("Hello World!"))
				.withOutput(new Text("Hello"), new IntWritable(1))
				.withOutput(new Text("World!"), new IntWritable(1))
				.runTest();
		} catch (IOException e) {
			fail();
		}
	}
	
	@Test
	public final void testReduce() {
		try {
			List<IntWritable> values = new ArrayList<IntWritable>();
			values.add(new IntWritable(1));
			values.add(new IntWritable(1));
			// @formatter:off
			reduceDriver.withInput(new Text("Hello"), values)
				.withOutput(new Text("Hello"), new IntWritable(2))
				.runTest();
		} catch (IOException e) {
			fail();
		}
	}
}
