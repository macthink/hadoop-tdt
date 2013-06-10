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
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * DocumentTFIDFReducer
 * 
 * @author Macthink
 */
public class DocumentTFIDFReducer extends Reducer<Text, Text, Text, Text> {

	/**
	 * LOG
	 */
	private static final Log LOG = LogFactory.getLog(DocumentTFIDFReducer.class);

	/**
	 * 格式化输出结果
	 */
	private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("###.#########");

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
	 * Reducer函数 Input:(word,[docNamei=ni/N,...]);Output:(docNamei,[wordi=tfidfi,...])
	 * 
	 * @param key
	 *            输入文档名称
	 * @param values
	 *            文档频率的键值对
	 * @param context
	 */
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// 将Values解析成Map(key:docName; value:n/N)
		Map<String, String> valueMap = new HashMap<String, String>();

		// 文档总数，即输入文件夹中的文件个数
		int numOfDocs = context.getConfiguration().getInt("numOfDocs", 0);

		// 语料中出现单词的文档的数量
		int numOfDocsWhereTermAppears = 0;

		// 计算语料中出现单词的文档的数量
		for (Text value : values) {
			String[] valueStrings = value.toString().split("=");
			// 当单词在该文档中的出现次数大于0
			if (Integer.parseInt(valueStrings[1].split("/")[0]) > 0) {
				numOfDocsWhereTermAppears++;
			}
			valueMap.put(valueStrings[0], valueStrings[1]);
		}
		// 计算当前单词对于某一文档的TDIDF
		for (String docName : valueMap.keySet()) {
			String[] tfStrings = valueMap.get(docName).split("/");

			// 计算Term Frequency
			double tf = Double.valueOf(Double.valueOf(tfStrings[0]) / Double.valueOf(tfStrings[1]));

			// 计算Inverse Document Frequency
			double idf = Math.log10((double) numOfDocs
					/ (double) ((numOfDocsWhereTermAppears == 0 ? 1 : 0) + numOfDocsWhereTermAppears));

			// 计算TDIDF
			double tfIdf = tf * idf;

			// 只有当TDIDF不等于0时才发射
			if (tfIdf != 0) {
				context.write(new Text(docName), new Text((key.toString() + "=" + DECIMAL_FORMAT.format(tfIdf))));
			}
		}
	}
}
