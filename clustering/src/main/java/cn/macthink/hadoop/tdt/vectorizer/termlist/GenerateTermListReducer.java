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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cn.macthink.hadoop.tdt.util.constant.Constants;

/**
 * GenerateTermListReducer
 * 
 * @author Macthink
 */
public class GenerateTermListReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	/**
	 * result
	 */
	private IntWritable result = new IntWritable();

	/**
	 * Reducer函数
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
			InterruptedException {
		// 求和，得到
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		result.set(sum);

		// 判断是否在设置的取值范围内
		if (result.get() > Constants.TERM_LIST_MIN_DF && result.get() < Constants.TERM_LIST_MAX_DF) {
			context.write(key, result);
		}
	}
}
