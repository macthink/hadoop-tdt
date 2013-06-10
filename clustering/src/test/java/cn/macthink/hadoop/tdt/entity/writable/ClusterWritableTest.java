/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-5-31
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.entity.writable;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import cn.macthink.hadoop.tdt.entity.Cluster;
import cn.macthink.hadoop.tdt.entity.Vector;

import com.google.common.collect.Lists;

/**
 * ClusterWritableTest
 * 
 * @author Macthink
 */
public class ClusterWritableTest {

	/**
	 * 从数据文件中读取数据
	 * 
	 * @param datas
	 *            存储数据的集合对象
	 * @param path
	 *            数据文件的路径
	 */
	@SuppressWarnings("resource")
	public static List<Vector> readVectors(String path) {
		try {
			List<Vector> vectors = Lists.newArrayList();
			BufferedReader br = new BufferedReader(new FileReader(new File(path)));
			String reader = br.readLine();
			while (reader != null) {
				String t[] = reader.split("\\s{1,}");
				ArrayList<Double> list = new ArrayList<Double>();
				for (int i = 0; i < t.length; i++) {
					list.add(Double.parseDouble(t[i]));
				}
				Vector vector = new Vector(list);
				vectors.add(vector);
				reader = br.readLine();
			}
			return vectors;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String basePath = Class.class.getClass().getResource("/").toString().replaceAll("file:/", "");
		String vectorFile = basePath + "data1";

		// 准备样本集合
		List<Vector> vectors = Lists.newArrayList();
		vectors = readVectors(vectorFile);
		Vector[] vectorArray = new Vector[vectors.size()];
		vectors.toArray(vectorArray);
		System.out.println(vectors.size());

		// 获得分区
		Cluster cluster = new Cluster();
		cluster.setVectors(vectors);
		ClusterWritable clusterWritable = new ClusterWritable(cluster);
		System.out.println(clusterWritable.getPartitionNum(5));

	}
}
