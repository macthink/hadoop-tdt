/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-6-17
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.clustering.agenes.step2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import cn.macthink.hadoop.tdt.entity.writable.ClusterDistanceWritable;
import cn.macthink.hadoop.tdt.util.partitionsort.PartitionSortKeyPair;

/**
 * MergeClustersMapper
 * 
 * @author Macthink
 */
public class MergeClustersMapper extends
		Mapper<PartitionSortKeyPair, ClusterDistanceWritable, IntWritable, ClusterDistanceWritable> {

	@Override
	protected void map(PartitionSortKeyPair key, ClusterDistanceWritable value, Context context) throws IOException,
			InterruptedException {
		context.write(key.getPartitionKey(), value);
	}

}
