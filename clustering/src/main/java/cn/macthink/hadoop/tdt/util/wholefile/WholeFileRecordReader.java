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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * WholeFileRecordReader
 * 
 * @author Macthink
 */
public class WholeFileRecordReader extends RecordReader<Text, Text> {

	private FileSplit fileSplit;
	private Configuration conf;
	private final Text currentKey = new Text();
	private final Text currentValue = new Text();
	private boolean fileProcessed = false;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		this.fileSplit = (FileSplit) split;
		this.conf = context.getConfiguration();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (fileProcessed) {
			return false;
		}
		int fileLength = (int) fileSplit.getLength();
		byte[] fileContent = new byte[fileLength];
		FileSystem fileSystem = FileSystem.get(conf);
		FSDataInputStream fsDataInputStream = null;
		try {
			currentKey.set(fileSplit.getPath().getName());
			fsDataInputStream = fileSystem.open(fileSplit.getPath());
			IOUtils.readFully(fsDataInputStream, fileContent, 0, fileLength);
			currentValue.set(fileContent, 0, fileLength);
		} finally {
			IOUtils.closeStream(fsDataInputStream);
		}
		this.fileProcessed = true;
		return true;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return currentKey;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return currentValue;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0;
	}

	@Override
	public void close() throws IOException {
	}
}
