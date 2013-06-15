/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-4-19
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.vectorizer.termlist;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cn.macthink.hadoop.tdt.util.HadoopUtils;
import cn.macthink.hadoop.tdt.util.constant.Constants;

/**
 * 生成词汇表
 * 
 * @author Macthink
 */
public class GenerateTermListDriver extends Configured implements Tool {

	/**
	 * LOG
	 */
	private static final Log LOG = LogFactory.getLog(GenerateTermListDriver.class);

	/**
	 * Run
	 */
	public int run(String[] args) throws Exception {

		// 输入参数校验
		if (StringUtils.isBlank(Constants.TERM_LIST_INPUT_PATH) || StringUtils.isBlank(Constants.TERM_LIST_OUTPUT_PATH)) {
			System.exit(1);
		}

		// 构造Configuration
		Configuration conf = getConf();
		conf.set(Constants.MAPRED_JOB_TRACKER_KEY, Constants.MAPRED_JOB_TRACKER);
		conf.set(Constants.FS_DEFAULT_NAME_KEY, Constants.FS_DEFAULT_NAME);

		// 获得文件系统及输入输出路径
		String fsDefaultName = conf.get(Constants.FS_DEFAULT_NAME_KEY);
		Path inputPath = new Path(fsDefaultName, Constants.TERM_LIST_INPUT_PATH);
		Path outputPath = new Path(fsDefaultName, Constants.TERM_LIST_OUTPUT_PATH);

		// 如果输出文件存在，则移除
		HadoopUtils.delete(conf, outputPath);

		// 打印输入信息
		LOG.info(inputPath);
		LOG.info(outputPath);

		// 配置Job
		Job job = new Job(conf, GenerateTermListDriver.class.getSimpleName());
		job.setJarByClass(GenerateTermListDriver.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapperClass(GenerateTermListMapper.class);
		job.setReducerClass(GenerateTermListReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

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
		int code = ToolRunner.run(new Configuration(), new GenerateTermListDriver(), args);
		System.exit(code);
	}

}
