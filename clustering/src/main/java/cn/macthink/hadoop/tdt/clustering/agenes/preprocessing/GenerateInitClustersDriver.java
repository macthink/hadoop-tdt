/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-5-30
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.clustering.agenes.preprocessing;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cn.macthink.hadoop.tdt.entity.writable.ClusterWritable;
import cn.macthink.hadoop.tdt.util.Constants;
import cn.macthink.hadoop.tdt.util.HadoopUtils;

/**
 * TextFileToSequenceFileDriver
 * 
 * @author Macthink
 */
public class GenerateInitClustersDriver extends Configured implements Tool {

	/**
	 * LOG
	 */
	private static final Log LOG = LogFactory.getLog(GenerateInitClustersDriver.class);

	/**
	 * Run
	 */
	@Override
	public int run(String[] args) throws Exception {

		// 输入参数校验
		if (StringUtils.isBlank(Constants.GENERATE_INIT_CLUSTERS_INPUT_PATH)
				|| StringUtils.isBlank(Constants.GENERATE_INIT_CLUSTERS_OUTPUT_PATH)) {
			System.exit(1);
		}

		// 构造Configuration
		Configuration conf = getConf();
		conf.set(Constants.MAPRED_JOB_TRACKER_KEY, Constants.MAPRED_JOB_TRACKER);
		conf.set(Constants.FS_DEFAULT_NAME_KEY, Constants.FS_DEFAULT_NAME);

		// 获得文件系统及输入、输出路径
		String fsDefaultName = conf.get(Constants.FS_DEFAULT_NAME_KEY);
		Path inputPath = new Path(fsDefaultName, Constants.GENERATE_INIT_CLUSTERS_INPUT_PATH);
		Path outputPath = new Path(fsDefaultName, Constants.GENERATE_INIT_CLUSTERS_OUTPUT_PATH);

		// 如果输出文件存在，则移除
		HadoopUtils.delete(conf, outputPath);

		// 打印输入信息
		LOG.info("inputPath is " + inputPath);
		LOG.info("outputPath is " + outputPath);

		// 配置Job
		Job job = new Job(conf, GenerateInitClustersDriver.class.getSimpleName());
		job.setJarByClass(GenerateInitClustersDriver.class);
		job.setNumReduceTasks(0);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapperClass(GenerateInitClustersMapper.class);

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
		int code = ToolRunner.run(new Configuration(), new GenerateInitClustersDriver(), args);
		System.exit(code);
	}

}
