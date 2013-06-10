/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-5-30
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.clustering.agenes;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import cn.macthink.hadoop.tdt.entity.Cluster;
import cn.macthink.hadoop.tdt.entity.ClusterDistance;
import cn.macthink.hadoop.tdt.entity.writable.ClusterDistanceWritable;
import cn.macthink.hadoop.tdt.entity.writable.ClusterWritable;

/**
 * AgglomerateReducer
 * 
 * @author Macthink
 */
public class AgglomerateReducer extends Reducer<DoubleWritable, ClusterDistanceWritable, NullWritable, ClusterWritable> {

	private float distanceThreshold;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		distanceThreshold = context.getConfiguration().getFloat("", 1.0f);
	}

	/**
	 * 从小到大遍历，如果小于距离阈值则合并类别
	 */
	@Override
	protected void reduce(DoubleWritable key, Iterable<ClusterDistanceWritable> values, Context context)
			throws IOException, InterruptedException {
		// 存放已进行合并的类别
		Set<String> combinedClusterSet = new HashSet<String>();
		// 遍历所有ClusterDistanceWritable
		Iterator<ClusterDistanceWritable> iterator = values.iterator();
		while (iterator.hasNext()) {
			ClusterDistance clusterDistance = iterator.next().get();
			// 判断是否与其他类别合并过
			if (combinedClusterSet.contains(clusterDistance.getSource().getId())
					|| combinedClusterSet.contains(clusterDistance.getTarget().getId())) {
				continue;
			}
			// 尚未与其他类别合并过
			double distance = clusterDistance.getDistance();
			if (distance >= 0.0d && distance < distanceThreshold) {
				// 合并类别
				Cluster newCluster = new Cluster(clusterDistance.getSource());
				newCluster.combineSamplePoints(clusterDistance.getTarget());
				ClusterWritable newClusterWritable = new ClusterWritable(newCluster);
				context.write(NullWritable.get(), newClusterWritable);

				combinedClusterSet.add(clusterDistance.getSource().getId());
				combinedClusterSet.add(clusterDistance.getTarget().getId());
			} else {
				ClusterWritable newClusterWritable = new ClusterWritable(clusterDistance.getSource());
				context.write(NullWritable.get(), newClusterWritable);
			}
		}

		// 经过上述处理之后，还有一些类别并没有添加到新类别集合中
		// for (Cluster category : categories) {
		// if (!combinedClusterSet.contains(category.getUuid())) {
		// context.write(key, value);
		// newCategories.add(category);
		// }
		// }
	}

}
