/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-5-13
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.distance.cluster;

import cn.macthink.hadoop.tdt.entity.Cluster;

/**
 * ClusterDistanceMeasure
 * 
 * @author Macthink
 */
public interface ClusterDistanceMeasure {

	/**
	 * 度量类别间的距离
	 * 
	 * @param category1
	 * @param category2
	 * @return
	 */
	double distance(Cluster category1, Cluster category2);
}
