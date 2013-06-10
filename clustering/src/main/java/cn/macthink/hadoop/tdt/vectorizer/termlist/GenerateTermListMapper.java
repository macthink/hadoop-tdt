/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-4-19
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.vectorizer.termlist;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cn.macthink.hadoop.tdt.util.Constants;

/**
 * GenerateTermListMapper
 * 
 * @author Macthink
 */
public class GenerateTermListMapper extends Mapper<Text, Text, Text, IntWritable> {

	/**
	 * singleCount
	 */
	private final static IntWritable singleCount = new IntWritable(1);

	/**
	 * word
	 */
	private Text word = new Text();

	/**
	 * Mapper函数
	 */
	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		// GBK转码
		String content = new String(value.getBytes(), 0, value.getLength(), Constants.INPUT_FILE_CHARSET_NAME);
		// 文件内容切分
		StringTokenizer stringTokenizer = new StringTokenizer(content);
		while (stringTokenizer.hasMoreTokens()) {
			word.set(stringTokenizer.nextToken());
			context.write(word, singleCount);
		}
	}
}
