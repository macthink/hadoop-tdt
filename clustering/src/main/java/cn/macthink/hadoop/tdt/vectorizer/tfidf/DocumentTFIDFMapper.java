/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-5-3
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.vectorizer.tfidf;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cn.macthink.hadoop.tdt.util.Constants;

/**
 * DocumentTFIDFMapper
 * 
 * @author Macthink
 */
public class DocumentTFIDFMapper extends Mapper<Text, Text, Text, Text> {

	/**
	 * LOG
	 */
	private static final Log LOG = LogFactory.getLog(DocumentTFIDFMapper.class);

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
	 * Map函数 Input:(docName,docContent);Output:(word,docName=n/N)
	 * 
	 * @param key
	 *            输入文档名称
	 * @param value
	 *            输入文档内容（整个文件）
	 * @param context
	 */
	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		// GBK转码
		String content = new String(value.getBytes(), 0, value.getLength(), Constants.INPUT_FILE_CHARSET_NAME);
		StringTokenizer stringTokenizer = new StringTokenizer(content.toString());
		// 文档的总单词数
		int wordNumInDoc = stringTokenizer.countTokens();
		// 遍历词汇表
		for (String word : termList) {
			// 词汇的频数
			int wordFrequence = 0;
			// 遍历文档
			stringTokenizer = new StringTokenizer(content.toString());
			while (stringTokenizer.hasMoreTokens()) {
				// 遍历词汇表
				if (stringTokenizer.nextToken().equals(word)) {
					wordFrequence++;
				}
			}
			// 发射结果
			context.write(new Text(word), new Text(key.toString() + "=" + wordFrequence + "/" + wordNumInDoc));
		}
	}
}
