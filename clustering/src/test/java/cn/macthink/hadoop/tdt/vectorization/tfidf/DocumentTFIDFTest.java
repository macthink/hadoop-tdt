package cn.macthink.hadoop.tdt.vectorization.tfidf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import cn.macthink.hadoop.tdt.vectorizer.tfidf.old.DocumentTFIDFMapper;
import cn.macthink.hadoop.tdt.vectorizer.tfidf.old.DocumentTFIDFReducer;

public class DocumentTFIDFTest {

	private Mapper<Text, Text, Text, Text> mapper;
	private Reducer<Text, Text, Text, Text> reducer;
	private MapDriver<Text, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;
	private MapReduceDriver<Text, Text, Text, Text, Text, Text> mapReduceDriver;

	@Before
	public void setUp() throws Exception {
		mapper = new DocumentTFIDFMapper();
		reducer = new DocumentTFIDFReducer();
		mapDriver = new MapDriver<Text, Text, Text, Text>(mapper);
		mapDriver.withCacheFile("word-list");
		reduceDriver = new ReduceDriver<Text, Text, Text, Text>(reducer);
		reduceDriver.withCacheFile("word-list");
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
		mapReduceDriver.withCacheFile("word-list");
	}

	@Test
	public final void testMapper() {
		try {
			Pair<Text, Text> pair = new Pair<Text, Text>(new Text("1f7a52a2f4d5fca1bdcc1fc1e88bb3f2.txt"), new Text(
					"手 短信 情况 奖金 宝马 儿子 女儿 总经理 头衔 亲戚 公司 法院 传票 电话费 信用卡"));
			mapDriver.addInput(pair);
			List<Pair<Text, Text>> pairs = mapDriver.run();
			for (Pair<Text, Text> pair1 : pairs) {
				System.out.println("( " + pair1.getFirst() + " , " + pair1.getSecond() + " )");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public final void testReducer() {
		try {
			List<Text> values = new ArrayList<Text>();
			values.add(new Text("docName=1/5"));
			values.add(new Text("docName=1/5"));
			Configuration conf = reduceDriver.getConfiguration();
			conf.setInt("numOfDocs", 1);
			List<Pair<Text, Text>> pairs = reduceDriver.withInput(new Text("一致"), values).run();
			for (Pair<Text, Text> pair : pairs) {
				System.out.println("( " + pair.getFirst() + " , " + pair.getSecond() + " )");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public final void testMapperReducer() {
		try {
			mapReduceDriver.withInput(new Text("docName0"), new Text(
					"长 知识 路遇 小 草莓 小贩 秤 问题 小贩 电子秤 重量 小贩 脸色 白 重量 三星 重 小米"));
			// .withInput(new Text("docName1"), new Text("最贵 全球 寰球 汽车 全球 最贵 黎巴嫩 售价 惊讶 售价 公里/小时 最高 车速 公里/小时"))
			// .withInput(new Text("docName2"), new Text("降点 交通运输 成本"));
			Configuration conf = mapReduceDriver.getConfiguration();
			conf.setInt("numOfDocs", 3);
			List<Pair<Text, Text>> pairs = mapReduceDriver.run();
			for (Pair<Text, Text> pair : pairs) {
				System.out.println("( " + pair.getFirst() + " , " + pair.getSecond() + " )");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
