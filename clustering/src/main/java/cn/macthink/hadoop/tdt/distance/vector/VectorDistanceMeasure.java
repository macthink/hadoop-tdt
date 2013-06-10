/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-5-13
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.distance.vector;

import cn.macthink.hadoop.tdt.entity.Vector;

/**
 * DistanceMeasure
 * 
 * @author Macthink
 */
public interface VectorDistanceMeasure {

	/**
	 * 度量样本点间的距离
	 * 
	 * @param samplePoint1
	 * @param samplePoint2
	 * @return
	 */
	double distance(Vector samplePoint1, Vector samplePoint2);
}
