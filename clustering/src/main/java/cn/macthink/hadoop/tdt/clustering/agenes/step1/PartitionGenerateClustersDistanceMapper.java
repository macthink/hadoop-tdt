/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-5-31
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.clustering.agenes.step1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import cn.macthink.hadoop.tdt.entity.writable.ClusterWritable;
import cn.macthink.hadoop.tdt.util.Constants;

/**
 * PartitionGenerateClustersDistanceMapper
 * 
 * @author Macthink
 */
public class PartitionGenerateClustersDistanceMapper extends
		Mapper<NullWritable, ClusterWritable, IntWritable, ClusterWritable> {

	// 将使用的处理机数目（处理机即Mapper or Reducer）
	private int processorNum;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		processorNum = context.getConfiguration().getInt(Constants.CLUSTERING_AGENES_PROCESSOR_NUM_KEY, 1);
	}

	/**
	 * 计算输入的簇应该划分到哪一个处理机中去
	 */
	@Override
	protected void map(NullWritable key, ClusterWritable value, Context context) throws IOException,
			InterruptedException {
		context.write(new IntWritable(value.getPartitionNum(processorNum)), value);
	}

}
