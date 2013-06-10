/**
 * Project:hadoop-tdt-preprocessing
 * File Created at 2013-4-28
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.preprocessing;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.macthink.hadoop.tdt.preprocessing.cli.PreprocessingCommandLine;

/**
 * IndexFileExtractor
 * 
 * @author Macthink
 */
public class IndexFileExtractor {

	private static Logger logger = LoggerFactory.getLogger(IndexFileExtractor.class);

	// 系统换行符 "\r\n" .index文件是在windows平台下生成
	private static final String lineSeparator = "\r\n";

	/**
	 * 解析.index文件
	 * 
	 * @param inputFile
	 * @param outputDirectory
	 * @param oldInputDirectory
	 */
	public static void extract(File inputFile, File oldInputDirectory, File outputDirectory) {

		logger.info("Input file is [ {} ].", inputFile.getAbsolutePath());

		// 读取文件
		String fileContent = null;
		try {
			fileContent = FileUtils.readFileToString(inputFile,
					Charset.forName(PreprocessingCommandLine.inputFileCharset));
		} catch (IOException e) {
			logger.error(e.getMessage());
		}

		// 分割文件中的记录
		String[] records = fileContent.split("<REC>" + IndexFileExtractor.lineSeparator);
		for (String record : records) {
			if (StringUtils.isBlank(record)) {
				continue;
			}
			Map<String, String> recordMap = IndexFileExtractor.extractIndexRecord(record);
			if (recordMap.containsKey("guid") && recordMap.containsKey("content")) {
				String pageUrl = recordMap.get("pageurl");
				String content = recordMap.get("content");
				if (StringUtils.isBlank(pageUrl) && StringUtils.isBlank(content)) {
					continue;
				}
				// 以pageurl的值作为文件名，新建输出文件，会覆盖掉旧文件
				String fileName = DigestUtils.md5Hex(pageUrl);
				File outputFile = new File(outputDirectory, fileName + ".txt");
				try {
					if (outputFile.createNewFile()) {
						FileUtils
								.write(outputFile, content, Charset.forName(PreprocessingCommandLine.inputFileCharset));
					}
				} catch (IOException e) {
					logger.error(e.getMessage());
					return;
				}
			}
		}

		// 解析结束之后，不管成功与否，都将其从输入目录中移走
		File oldInputFile = new File(oldInputDirectory, inputFile.getName());
		if (oldInputFile.exists()) {
			oldInputFile.delete();
		}
		try {
			FileUtils.moveFileToDirectory(inputFile, oldInputDirectory, true);
		} catch (IOException e) {
			logger.error(e.getMessage());
			return;
		}
	}

	/**
	 * 将每一条记录解析成Map对象
	 * 
	 * @param record
	 * @return
	 */
	private static Map<String, String> extractIndexRecord(String record) {
		Map<String, String> recordMap = new HashMap<String, String>();
		String[] keyValuePairs = record.split(IndexFileExtractor.lineSeparator);
		for (String keyValuePair : keyValuePairs) {
			if (StringUtils.isEmpty(keyValuePair)) {
				continue;
			}
			int position = keyValuePair.indexOf(">=");
			if (position == -1) {
				continue;
			}
			String key = keyValuePair.substring(1, position).toLowerCase();
			String value = keyValuePair.substring(position + 2).trim().replaceAll("> >", "");
			recordMap.put(key, value);
		}
		return recordMap;
	}
}
