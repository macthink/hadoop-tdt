/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-5-5
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.util.compound;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * CompoundKeyPartitioner
 * 
 * @author Macthink
 */
public class CompoundKeyValuePairPartitioner<K extends WritableComparable<K>, V extends WritableComparable<V>> extends
		Partitioner<CompoundKeyValuePair<K, V>, Writable> {

	@Override
	public int getPartition(CompoundKeyValuePair<K, V> key, Writable value, int numPartitions) {
		return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}
