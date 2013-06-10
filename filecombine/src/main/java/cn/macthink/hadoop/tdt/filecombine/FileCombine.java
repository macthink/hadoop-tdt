/**
 * Project:hadoop-tdt-filecombine
 * File Created at 2013-5-10
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.filecombine;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.macthink.hadoop.tdt.filecombine.cli.FileCombineCommandLine;

/**
 * FileCombine
 * 
 * @author Macthink
 */
public class FileCombine {

	private static Logger logger = LoggerFactory.getLogger(FileCombine.class);

	private static String lineSeparator = System.getProperty("line.separator");

	/**
	 * 将输入的单个文本文件为合并到指定的大文本中，把该文本文件的文件名写入行首，用制表符与内容隔开。
	 * 
	 * @param inputFile
	 * @param outputFile
	 */
	public static void combine(File inputFile, File outputFile) {

		logger.info("Input file is [ {} ].", inputFile.getAbsolutePath());

		// 读取文件
		String fileName = null;
		String fileContent = null;
		try {
			// 读取文件名（去掉扩展名）和文件内容
			String tmpFileName = inputFile.getName();
			fileName = tmpFileName.substring(0, tmpFileName.lastIndexOf("."));
			fileContent = FileUtils.readFileToString(inputFile,
					Charset.forName(FileCombineCommandLine.inputFileCharset)).replaceAll("\r|\n", "");

			if (StringUtils.isBlank(fileName) || StringUtils.isBlank(fileContent)) {
				return;
			}
			// 组合新文件内容
			StringBuffer newFileContent = new StringBuffer();
			newFileContent.append(fileName).append("\t").append(fileContent).append(lineSeparator);

			// 将新文件内容写出到输出文件
			FileUtils.writeStringToFile(outputFile, newFileContent.toString(),
					Charset.forName(FileCombineCommandLine.inputFileCharset), true);

		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}
}
