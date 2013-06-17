/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-06-16
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.clustering.agenes.step2;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cn.macthink.hadoop.tdt.entity.writable.ClusterDistanceWritable;
import cn.macthink.hadoop.tdt.entity.writable.ClusterWritable;
import cn.macthink.hadoop.tdt.util.HadoopUtils;
import cn.macthink.hadoop.tdt.util.constant.Constants;
import cn.macthink.hadoop.tdt.util.partitioner.KeyPartitioner;

/**
 * MergeClustersDriver
 * 
 * @author Macthink
 */
public class MergeClustersDriver extends Configured implements Tool {

	/**
	 * LOG
	 */
	private static final Log LOG = LogFactory.getLog(MergeClustersDriver.class);

	/**
	 * Run
	 */
	@Override
	public int run(String[] args) throws Exception {

		// 输入参数校验
		if (StringUtils.isBlank(Constants.MERGE_CLUSTERS_INPUT_PATH)
				|| StringUtils.isBlank(Constants.MERGE_CLUSTERS_OUTPUT_PATH)) {
			System.exit(1);
		}

		// 构造Configuration
		Configuration conf = getConf();
		conf.set(Constants.MAPRED_JOB_TRACKER_KEY, Constants.MAPRED_JOB_TRACKER);
		conf.set(Constants.FS_DEFAULT_NAME_KEY, Constants.FS_DEFAULT_NAME);
		// 设置处理机数目
		conf.setInt(Constants.CLUSTERING_AGENES_PROCESSOR_NUM_KEY, Constants.CLUSTERING_AGENES_PROCESSOR_NUM);
		// 设置类别间距离阈值
		conf.set(Constants.DISTANCE_THRESHOLD_KEY, Constants.DISTANCE_THRESHOLD);

		// 获得文件系统及输入、输出路径
		String fsDefaultName = conf.get(Constants.FS_DEFAULT_NAME_KEY);
		Path inputPath = new Path(fsDefaultName, Constants.MERGE_CLUSTERS_INPUT_PATH);
		Path outputPath = new Path(fsDefaultName, Constants.MERGE_CLUSTERS_OUTPUT_PATH);

		// 如果输出文件存在，则移除
		HadoopUtils.delete(conf, outputPath);

		// 打印输入信息
		LOG.info("inputPath is " + inputPath);
		LOG.info("outputPath is " + outputPath);

		// 配置Job
		Job job = new Job(conf, MergeClustersDriver.class.getSimpleName());
		job.setJarByClass(MergeClustersDriver.class);
		job.setNumReduceTasks(Constants.CLUSTERING_AGENES_PROCESSOR_NUM);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(ClusterDistanceWritable.class);

		job.setMapperClass(MergeClustersMapper.class);
		job.setPartitionerClass(KeyPartitioner.class);
		job.setReducerClass(MergeClustersReducer.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(ClusterWritable.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * Main
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		int code = ToolRunner.run(new Configuration(), new MergeClustersDriver(), args);
		System.exit(code);
	}
}
