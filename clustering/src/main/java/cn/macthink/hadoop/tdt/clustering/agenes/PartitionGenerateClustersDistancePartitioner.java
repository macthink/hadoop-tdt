/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-6-6
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.clustering.agenes;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import cn.macthink.hadoop.tdt.entity.writable.ClusterWritable;

/**
 * PartitionGenerateClustersDistancePartitioner
 * 
 * @author Macthink
 */
public class PartitionGenerateClustersDistancePartitioner extends Partitioner<IntWritable, ClusterWritable> {

	@Override
	public int getPartition(IntWritable key, ClusterWritable value, int numPartitions) {
		return key.get() % numPartitions;
	}

}
