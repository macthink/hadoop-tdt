/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-5-13
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.distance.cluster;

import cn.macthink.hadoop.tdt.distance.vector.VectorDistanceMeasure;

/**
 * ClusterAbstractDistanceMeasure
 * 
 * @author Macthink
 */
public abstract class ClusterAbstractDistanceMeasure implements ClusterDistanceMeasure {

	/**
	 * 样本点的距离度量策略
	 */
	protected VectorDistanceMeasure vectorDistanceMeasure;

	/**
	 * 构造函数
	 */
	public ClusterAbstractDistanceMeasure() {
		super();
	}

	/**
	 * 构造函数
	 * 
	 * @param samplePointDistanceMeasure
	 */
	public ClusterAbstractDistanceMeasure(VectorDistanceMeasure vectorDistanceMeasure) {
		super();
		this.vectorDistanceMeasure = vectorDistanceMeasure;
	}

}
