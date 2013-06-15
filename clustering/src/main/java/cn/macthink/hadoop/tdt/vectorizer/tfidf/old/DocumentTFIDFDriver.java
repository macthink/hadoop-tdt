/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-5-3
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.vectorizer.tfidf.old;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cn.macthink.hadoop.tdt.util.constant.Constants;

/**
 * 计算文档TFIDF
 * 
 * @author Macthink
 */
public class DocumentTFIDFDriver extends Configured implements Tool {

	/**
	 * LOG
	 */
	private static final Log LOG = LogFactory.getLog(DocumentTFIDFDriver.class);

	/**
	 * Run
	 */
	@Override
	public int run(String[] args) throws Exception {

		// 输入参数校验
		if (StringUtils.isBlank(Constants.TERM_LIST_PATH) || StringUtils.isBlank(Constants.DOCUMENT_TFIDF_INPUT_PATH)
				|| StringUtils.isBlank(Constants.DOCUMENT_TFIDF_OUTPUT_PATH)) {
			System.exit(1);
		}

		// 构造Configuration
		Configuration conf = getConf();
		conf.set(Constants.MAPRED_JOB_TRACKER_KEY, Constants.MAPRED_JOB_TRACKER);
		conf.set(Constants.FS_DEFAULT_NAME_KEY, Constants.FS_DEFAULT_NAME);

		// 获得文件系统及词汇表、输入、输出路径
		FileSystem fileSystem = FileSystem.get(conf);
		String fsDefaultName = conf.get(Constants.FS_DEFAULT_NAME_KEY);
		Path termListPath = new Path(fsDefaultName, Constants.TERM_LIST_PATH);
		Path inputPath = new Path(fsDefaultName, Constants.DOCUMENT_TFIDF_INPUT_PATH);
		Path outputPath = new Path(fsDefaultName, Constants.DOCUMENT_TFIDF_OUTPUT_PATH);

		// 统计输入文档总数
		FileStatus[] fileStatusList = fileSystem.listStatus(inputPath);
		int numOfDocs = 0;
		for (FileStatus fileStatus : fileStatusList) {
			Path path = fileStatus.getPath();
			InputStream inputStream = fileSystem.open(path);
			InputStreamReader inputStreamReader = new InputStreamReader(inputStream, Constants.INPUT_FILE_CHARSET_NAME);
			BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
			while (bufferedReader.readLine() != null) {
				numOfDocs++;
			}
		}
		conf.setInt("numOfDocs", numOfDocs);

		// 打印输入信息
		LOG.info("wordListPath is " + termListPath);
		LOG.info("inputPath is " + inputPath);
		LOG.info("outputPath is " + outputPath);
		LOG.info("numOfDocs is " + numOfDocs);

		// 如果输出文件存在，则移除
		if (fileSystem.exists(outputPath)) {
			fileSystem.delete(outputPath, true);
		}

		// DistributedCache.addCacheFile需要写在Job类初始化之前，否则在运行会中找不到文件
		DistributedCache.addCacheFile(termListPath.toUri(), conf);
		// 配置Job
		Job job = new Job(conf, DocumentTFIDFDriver.class.getSimpleName());
		job.setJarByClass(DocumentTFIDFDriver.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapperClass(DocumentTFIDFMapper.class);
		job.setReducerClass(DocumentTFIDFReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

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
		int code = ToolRunner.run(new Configuration(), new DocumentTFIDFDriver(), args);
		System.exit(code);
	}

}
