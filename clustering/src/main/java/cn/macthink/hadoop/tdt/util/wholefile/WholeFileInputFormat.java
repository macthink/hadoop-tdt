/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-5-2
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.util.wholefile;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * WholeFileInputFormat
 * 
 * @author Macthink
 */
public class WholeFileInputFormat extends FileInputFormat<Text, Text> {
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		// 以文件为单位，每个单位作为一个Split，即使单个文件的大小超过HDFS的一个块大小，也不进行分片
		return false;
	}

	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		WholeFileRecordReader reader = new WholeFileRecordReader();
		reader.initialize(split, context);
		return reader;
	}
}
