/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-6-5
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.util.partitionsort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * PartitionSortKeyPair
 * 
 * @author Macthink
 */
public class PartitionSortKeyPair implements WritableComparable<PartitionSortKeyPair> {

	/**
	 * 排序Key
	 */
	private DoubleWritable sortKey;
	/**
	 * 分区Key
	 */
	private IntWritable partitionKey;

	/**
	 * 构造函数
	 */
	public PartitionSortKeyPair() {
		super();
	}

	/**
	 * 构造函数
	 * 
	 * @param sortKey
	 * @param partitionKey
	 */
	public PartitionSortKeyPair(DoubleWritable sortKey, IntWritable partitionKey) {
		super();
		this.sortKey = sortKey;
		this.partitionKey = partitionKey;
	}

	/**
	 * @return the sortKey
	 */
	public DoubleWritable getSortKey() {
		return sortKey;
	}

	/**
	 * @return the partitionKey
	 */
	public IntWritable getPartitionKey() {
		return partitionKey;
	}

	/**
	 * @param sortKey
	 *            the sortKey to set
	 */
	public void setSortKey(DoubleWritable sortKey) {
		this.sortKey = sortKey;
	}

	/**
	 * @param partitionKey
	 *            the partitionKey to set
	 */
	public void setPartitionKey(IntWritable partitionKey) {
		this.partitionKey = partitionKey;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		sortKey.write(out);
		partitionKey.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		sortKey.readFields(in);
		partitionKey.readFields(in);
	}

	@Override
	public int compareTo(PartitionSortKeyPair o) {
		return sortKey.compareTo(o.sortKey);
	}

}
