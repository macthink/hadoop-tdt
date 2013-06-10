/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-5-5
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.vectorizer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cn.macthink.hadoop.tdt.vectorizer.tfidf.DocumentTFIDFReducer;

import com.google.common.collect.Maps;

/**
 * DocumentVectorizationReducer
 * 
 * @author Macthink
 */
public class DocumentVectorizationReducer extends Reducer<Text, Text, Text, Text> {

	/**
	 * LOG
	 */
	private static final Log LOG = LogFactory.getLog(DocumentTFIDFReducer.class);

	/**
	 * 词汇表
	 */
	private List<String> termList = new ArrayList<String>();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		try {
			// 读取词汇表
			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			if (null != cacheFiles && cacheFiles.length > 0) {
				String line = StringUtils.EMPTY;
				BufferedReader bufferedReader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
				try {
					while ((line = bufferedReader.readLine()) != null) {
						String[] keyValuePairs = line.split("\t", 2);
						termList.add(keyValuePairs[0]);
					}
				} finally {
					bufferedReader.close();
				}
			}
		} catch (IOException e) {
			LOG.error(e.getMessage());
		}
	}

	/**
	 * Reducer函数 Input:(docName,[wordi=tfidfi]);Output:(docName,[tfidf1,tfidf2,tfidf3,...,tfidfn])
	 * 
	 * @param key
	 *            输入文档名称
	 * @param values
	 *            文档TDIDF的键值对
	 * @param context
	 */
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		// TFIDF不为零的词汇及其特征值的Map
		Map<String, Double> existTermMap = Maps.newHashMap();

		for (Text value : values) {
			String[] tmpArray = value.toString().split("=");
			existTermMap.put(tmpArray[0], Double.valueOf(tmpArray[1]));
		}

		StringBuffer stringBuffer = new StringBuffer();
		for (String term : termList) {
			double termValue = 0.0d;
			if (existTermMap.containsKey(term)) {
				termValue = existTermMap.get(term);
			}
			stringBuffer.append(termValue).append(" ");
		}
		context.write(key, new Text(stringBuffer.toString()));
	}
}
