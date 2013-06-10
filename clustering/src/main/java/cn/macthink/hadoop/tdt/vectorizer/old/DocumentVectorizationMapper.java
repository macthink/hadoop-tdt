/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-5-5
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.vectorizer.old;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cn.macthink.hadoop.tdt.util.compound.CompoundKeyValuePair;

/**
 * DocumentVectorizationMapper：二次排序
 * 
 * @author Macthink
 */
public class DocumentVectorizationMapper extends Mapper<Text, Text, CompoundKeyValuePair<Text, Text>, Text> {

	/**
	 * Mapper函数 Input:(docName,[wordi=tfidfi]);Output:(<docName,word>,[tfidfi])
	 * 
	 * @param key
	 *            输入文档名称
	 * @param values
	 *            文档TDIDF的键值对
	 * @param context
	 */
	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		String[] valueStrings = value.toString().split("=");
		CompoundKeyValuePair<Text, Text> compoundKeyValuePair = new CompoundKeyValuePair<Text, Text>(key, new Text(
				valueStrings[0]));
		context.write(compoundKeyValuePair, new Text(valueStrings[1]));
	}
}
