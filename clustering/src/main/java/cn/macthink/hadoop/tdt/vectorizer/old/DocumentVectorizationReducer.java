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
import org.apache.hadoop.mapreduce.Reducer;

import cn.macthink.hadoop.tdt.util.compound.CompoundKeyValuePair;

/**
 * DocumentVectorizationReducer
 * 
 * @author Macthink
 */
public class DocumentVectorizationReducer extends Reducer<CompoundKeyValuePair<Text, Text>, Text, Text, Text> {

	/**
	 * Reducer函数 Input:(<docName,wordi>,tfidfi);Output:(docName,[tfidf1,tfidf2,tfidf3,...,tfidfn])
	 * 
	 * @param key
	 *            输入文档名称
	 * @param values
	 *            文档TDIDF的键值对
	 * @param context
	 */
	@Override
	protected void reduce(CompoundKeyValuePair<Text, Text> key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		StringBuffer stringBuffer = new StringBuffer();
		for (Text value : values) {
			stringBuffer.append(value.toString()).append(" ");
		}
		context.write(key.getFirst(), new Text(stringBuffer.toString()));
	}
}
