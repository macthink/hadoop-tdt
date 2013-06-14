/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-5-31
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.entity.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import cn.macthink.hadoop.tdt.entity.ClusterDistance;

/**
 * ClusterDistanceWritable
 * 
 * @author Macthink
 */
public class ClusterDistanceWritable implements WritableComparable<ClusterDistance> {

	/**
	 * ClusterDistance
	 */
	private ClusterDistance clusterDistance;

	/**
	 * 构造函数
	 */
	public ClusterDistanceWritable() {
		super();
	}

	/**
	 * 构造函数
	 * 
	 * @param clusterDistance
	 */
	public ClusterDistanceWritable(ClusterDistance clusterDistance) {
		super();
		this.clusterDistance = clusterDistance;
	}

	/**
	 * @return the clusterDistance
	 */
	public ClusterDistance get() {
		return clusterDistance;
	}

	/**
	 * @param clusterDistance
	 *            the clusterDistance to set
	 */
	public void set(ClusterDistance clusterDistance) {
		this.clusterDistance = clusterDistance;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(clusterDistance.getDistance());
		ClusterWritable.writeCluster(out, clusterDistance.getSource());
		ClusterWritable.writeCluster(out, clusterDistance.getTarget());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		clusterDistance.setDistance(in.readDouble());
		clusterDistance.setSource(ClusterWritable.readCluster(in));
		clusterDistance.setTarget(ClusterWritable.readCluster(in));
	}

	@Override
	public int compareTo(ClusterDistance o) {
		return clusterDistance.compareTo(o);
	}

	@Override
	public int hashCode() {
		return clusterDistance.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		return obj instanceof ClusterDistanceWritable && clusterDistance.equals(((ClusterDistanceWritable) obj).get());
	}

	@Override
	public String toString() {
		return clusterDistance.toString();
	}

}
