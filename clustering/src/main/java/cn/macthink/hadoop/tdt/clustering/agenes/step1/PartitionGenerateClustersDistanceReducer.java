/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-5-31
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.clustering.agenes.step1;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import cn.macthink.hadoop.tdt.clustering.agenes.step1.partitionsort.PartitionSortKeyPair;
import cn.macthink.hadoop.tdt.distance.cluster.ClusterAbstractDistanceMeasure;
import cn.macthink.hadoop.tdt.distance.vector.VectorAbstractDistanceMeasure;
import cn.macthink.hadoop.tdt.entity.ClusterDistance;
import cn.macthink.hadoop.tdt.entity.writable.ClusterDistanceWritable;
import cn.macthink.hadoop.tdt.entity.writable.ClusterWritable;
import cn.macthink.hadoop.tdt.util.ClassUtils;
import cn.macthink.hadoop.tdt.util.constant.Constants;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * GenerateClustersDistanceReducer
 * 
 * @author Macthink
 */
public class PartitionGenerateClustersDistanceReducer extends
		Reducer<IntWritable, ClusterWritable, PartitionSortKeyPair, ClusterDistanceWritable> {

	private VectorAbstractDistanceMeasure vectorDistanceMeasure;
	private ClusterAbstractDistanceMeasure clusterDistanceMeasure;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// 生成距离度量策略
		Configuration conf = context.getConfiguration();
		vectorDistanceMeasure = ClassUtils.instantiateAs(conf.get(Constants.VECTOR_DISTANCE_MEASURE_KEY),
				VectorAbstractDistanceMeasure.class);
		clusterDistanceMeasure = ClassUtils.instantiateAs(conf.get(Constants.CLUSTER_DISTANCE_MEASURE_KEY),
				ClusterAbstractDistanceMeasure.class);
		clusterDistanceMeasure.setVectorDistanceMeasure(vectorDistanceMeasure);
	}

	/**
	 * Reducer函数：生成类别间距离，相当于类别间的距离矩阵的上三角部分降成一维的
	 * 
	 * @param key
	 * @param values
	 * @param context
	 */
	@Override
	protected void reduce(IntWritable key, Iterable<ClusterWritable> values, Context context) throws IOException,
			InterruptedException {
		Preconditions.checkNotNull(clusterDistanceMeasure);

		// 将所有的ClusterWritable组成一个List
		List<ClusterWritable> clusterWritableList = Lists.newArrayList();
		Iterator<ClusterWritable> iterator = values.iterator();
		while (iterator.hasNext()) {
			clusterWritableList.add(iterator.next());
		}

		// 将List转换成数组，方便读取
		ClusterWritable[] clusterWritables = new ClusterWritable[clusterWritableList.size()];
		clusterWritableList.toArray(clusterWritables);

		// 生成类别间距离
		int length = clusterWritables.length;
		for (int i = 0; i < length - 1; i++) {
			ClusterWritable minClusterWritable = clusterWritables[i + 1];
			double minDistance = clusterDistanceMeasure.distance(clusterWritables[i].get(),
					clusterWritables[i + 1].get());
			for (int j = i + 2; j < length; j++) {
				double distance = clusterDistanceMeasure.distance(clusterWritables[i].get(), clusterWritables[j].get());
				if (minDistance > distance) {
					minClusterWritable = clusterWritables[j];
					minDistance = distance;
				}
			}
			ClusterDistance clusterDistance = new ClusterDistance(clusterWritables[i].get(), minClusterWritable.get(),
					minDistance);
			ClusterDistanceWritable clusterDistanceWritable = new ClusterDistanceWritable(clusterDistance);
			PartitionSortKeyPair partitionSortKeyPairWritable = new PartitionSortKeyPair(
					new DoubleWritable(minDistance), key);
			context.write(partitionSortKeyPairWritable, clusterDistanceWritable);
		}
	}
}
