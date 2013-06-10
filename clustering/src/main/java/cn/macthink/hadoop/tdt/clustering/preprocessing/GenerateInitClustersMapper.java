/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-5-30
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.clustering.preprocessing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cn.macthink.hadoop.tdt.entity.Cluster;
import cn.macthink.hadoop.tdt.entity.Vector;
import cn.macthink.hadoop.tdt.entity.writable.ClusterWritable;

/**
 * TextFileToSequenceFileMapper
 * 
 * @author Macthink
 */
public class GenerateInitClustersMapper extends Mapper<Text, Text, NullWritable, ClusterWritable> {

	/**
	 * SPACE
	 */
	private static final Pattern SPACE = Pattern.compile(" ");

	/**
	 * Map函数 Input:(docName,docVector); Output:(NullWritable,ClusterWritable).
	 * 
	 * @param key
	 *            输入文档名称
	 * @param value
	 *            输入文档的向量表示
	 * @param context
	 */
	@Override 
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		String[] numbers = GenerateInitClustersMapper.SPACE.split(value.toString());
		List<Double> doubles = new ArrayList<Double>();
		for (String number : numbers) {
			if (number.length() > 0) {
				doubles.add(Double.valueOf(number));
			}
		}
		// 忽略空行
		if (!doubles.isEmpty()) {
			Vector vector = new Vector(doubles);
			Cluster cluster = new Cluster(vector);
			ClusterWritable clusterWritable = new ClusterWritable(cluster);
			context.write(NullWritable.get(), clusterWritable);
		}
	}

}
