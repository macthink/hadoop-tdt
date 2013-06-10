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
import cn.macthink.hadoop.tdt.entity.Cluster;
import cn.macthink.hadoop.tdt.entity.Vector;

/**
 * ClusterMeanDistanceMeasure
 * 
 * @author Macthink
 */
public class ClusterMeanDistanceMeasure extends ClusterAbstractDistanceMeasure {

	public ClusterMeanDistanceMeasure(VectorDistanceMeasure samplePointDistanceMeasure) {
		super(samplePointDistanceMeasure);
	}

	@Override
	public double distance(Cluster category1, Cluster category2) {
		Vector meanCentroid1 = category1.getMeanCentroid();
		Vector meanCentroid2 = category2.getMeanCentroid();
		return vectorDistanceMeasure.distance(meanCentroid1, meanCentroid2);
	}

}
