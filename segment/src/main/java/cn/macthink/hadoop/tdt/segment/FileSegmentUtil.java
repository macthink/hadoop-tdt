/**
 * Project:hadoop-tdt-segment
 * File Created at 2013-4-27
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.segment;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.macthink.hadoop.tdt.segment.cli.SegmentCommandLine;
import cn.macthink.segment.Segment;
import cn.macthink.segment.impl.StandardSegmentImpl;
import cn.macthink.segment.newword.NewWordExtractor;

/**
 * FileSegmentUtil
 * 
 * @author Macthink
 */
public class FileSegmentUtil {

	private static Logger logger = LoggerFactory.getLogger(FileSegmentUtil.class);

	private static Segment segment = new StandardSegmentImpl();

	public static void initializeSegment() {
		logger.info("Starting initialize segmentation tool.");
		// 初始化分词器，之后的分词就不用再重新初始化
		if (segment.segment("initialize") != null) {
			logger.info("Success to initialize segmentation tool.");
		} else {
			logger.error("Fail to initialize segmentation tool.");
			System.exit(1);
		}
	}

	/**
	 * 文件分词执行
	 * 
	 * @param inputFile
	 * @param oldInputDirectory
	 * @param outputDirectory
	 * @param extractNewWord
	 * @param newWordFile
	 */
	public static void execute(File inputFile, File oldInputDirectory, File outputDirectory, boolean extractNewWord,
			File newWordOutputFile) {

		// 输入文件校验
		if (inputFile == null || !inputFile.canRead()) {
			return;
		}
		// TODO 判断输入文件是不是文本文件

		// 新建输出文件，会覆盖掉旧文件
		File outputFile = new File(outputDirectory, inputFile.getName());
		try {
			outputFile.createNewFile();
		} catch (IOException e) {
			logger.error(e.getMessage());
			return;
		}

		// 开始分词
		try {
			logger.info("Input file is [ {} ], Output file is [ {} ].", inputFile.getAbsolutePath(),
					outputFile.getAbsolutePath());
			segment.segmentFileByPos(inputFile.getAbsolutePath(), outputFile.getAbsolutePath(),
					SegmentCommandLine.posSet, SegmentCommandLine.inputFileCharsetEnum);
			if (extractNewWord) {
				logger.info("New word output file is [ {} ].", newWordOutputFile.getAbsolutePath());
				NewWordExtractor.extractFile(inputFile.getAbsolutePath(), newWordOutputFile.getAbsolutePath(),
						SegmentCommandLine.inputFileCharsetEnum);
			}
		} catch (UnsupportedEncodingException e) {
			logger.error(e.getMessage());
			return;
		} catch (IOException e) {
			logger.error(e.getMessage());
			return;
		}

		// 分词结束之后，不管成功与否，都将其从输入目录中移走
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
}
