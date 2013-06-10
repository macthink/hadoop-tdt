/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-5-7
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.clustering;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.iterator.ClusterWritable;

/**
 * SequenceFileOperation
 * 
 * @author Macthink
 */
public class SequenceFileOperation {

	/**
	 * 从sequence file文件中读取数据
	 * 
	 * @param sequeceFilePath
	 * @param conf
	 * @return
	 */
	public static List<ClusterWritable> readSequenceFile(String sequeceFilePath, Configuration conf) {
		List<ClusterWritable> result = null;
		FileSystem fs = null;
		SequenceFile.Reader reader = null;
		Path path = null;
		Writable key = null;
		ClusterWritable value = new ClusterWritable();

		try {
			fs = FileSystem.get(conf);
			result = new ArrayList<ClusterWritable>();
			path = new Path(sequeceFilePath);
			reader = new SequenceFile.Reader(fs, path, conf);
			key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf); // 获得Key，也就是之前写入的userId
			while (reader.next(key, value)) {
				result.add(value);
				value = new ClusterWritable();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(reader);
		}
		return result;
	}

	private static Configuration getDefaultConf() {
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "local");
		conf.set("fs.default.name", "file:///");
		return conf;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String basePath = Class.class.getClass().getResource("/").toString().replaceAll("file:/", "");
		String filePath = basePath + "part-r-00000"; // 文件路径
		List<ClusterWritable> readDatas = readSequenceFile(filePath, getDefaultConf());

		// 对比数据是否正确并输出
		for (ClusterWritable clusterWritable : readDatas) {
			Cluster cluster = clusterWritable.getValue();
			System.out.println(cluster.asFormatString(null));
		}

	}
}
