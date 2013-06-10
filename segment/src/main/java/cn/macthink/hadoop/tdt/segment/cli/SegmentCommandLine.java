/**
 * Project:hadoop-tdt-segment
 * File Created at 2013-4-22
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.segment.cli;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.macthink.hadoop.tdt.segment.FileSegmentUtil;
import cn.macthink.hadoop.tdt.segment.monitor.ExtensionFileFilter;
import cn.macthink.hadoop.tdt.segment.monitor.FileCreateListenerAdaptor;
import cn.macthink.segment.util.CharsetEnum;

/**
 * FolderSegment：文件夹文件批分词
 * 
 * @author Macthink
 */
public class SegmentCommandLine {

	private static Logger logger = LoggerFactory.getLogger(SegmentCommandLine.class);

	// 程序信息
	private static final String APP_NAME = "segment";
	private static final String HEADER = "Segment - A auto segment tool for TDT, Copyright 2013 Macthink.";
	private static final String FOOTER = "For more information, see: macthink.cn. Bug Reports to <macthink@hqu.edu.cn>";

	// 需要的输入参数
	public static String inputDirectoryPath = null;
	public static String outputDirectoryPath = null;
	public static String oldInputDirectoryPath = null;
	public static Boolean extractNewWord = null;
	public static String newWordOutputFilePath = null;
	public static String inputFileCharset = null;

	// 输入文件的编码
	public static CharsetEnum inputFileCharsetEnum = null;
	// 需要处理的输入文件的后缀名列表
	public static Set<String> inputFileExtensionSet = new HashSet<String>();
	// 分词时需要的词性
	public static Set<String> posSet = new HashSet<String>();
	// 文件监控扫描间隔
	private static final int interval = 1000;
	// 命令行宽度
	private static final int cliWidth = 80;

	/**
	 * 主函数
	 * 
	 * @param args
	 */
	@SuppressWarnings("static-access")
	public static void main(String[] args) {

		// 构造命令行解析器
		CommandLineParser commandLineParser = new BasicParser();
		Options options = new Options();
		// TODO 增加参数r表示遍历子文件夹
		//@formatter:off
		//iOption
		OptionGroup optionGroup = new OptionGroup();		
		Option iOption = OptionBuilder.withLongOpt("input")
				.hasArg(true)
				.withArgName("dirPath")
				.withDescription("Absolute path of input directory")
				.isRequired(true)
				.create("i");
		optionGroup.addOption(iOption);
		options.addOptionGroup(optionGroup);
		//oOption
		optionGroup = new OptionGroup();
		Option oOption = OptionBuilder.withLongOpt("output")
				.hasArg(true)
				.withArgName("dirPath")
				.withDescription("Absolute path of output directory")
				.isRequired(true)
				.create("o");
		optionGroup.addOption(oOption);
		options.addOptionGroup(optionGroup);
		//oiOption
		optionGroup = new OptionGroup();
		Option oiOption = OptionBuilder.withLongOpt("oldInput")
				.hasArg(true)
				.withArgName("dirPath")
				.withDescription("Absolute path of old output directory")
				.isRequired(true)
				.create("oi");
		optionGroup.addOption(oiOption);	
		options.addOptionGroup(optionGroup);
		//enwOption
		optionGroup = new OptionGroup();
		Option enwOption = OptionBuilder.withLongOpt("extractNewWord")
				.hasArg(true)
				.withArgName("true/false")
				.withDescription("The switch of extract new word")
				.isRequired(true)
				.create("enw");
		optionGroup.addOption(enwOption);
		options.addOptionGroup(optionGroup);
		//nwofOption
		optionGroup = new OptionGroup();
		Option nwofOption = OptionBuilder.withLongOpt("newWordOutput")
				.hasArg(true)
				.withArgName("filePath")
				.withDescription("Absolute path of new word output file")
				.create("nwof");
		optionGroup.addOption(nwofOption);	
		options.addOptionGroup(optionGroup);
		//cOption
		optionGroup = new OptionGroup();
		Option cOption = OptionBuilder.withLongOpt("charset")
				.hasArg(true)
				.withArgName("gbk/utf8")
				.withDescription("Charset of the input files")
				.create("c");
		optionGroup.addOption(cOption);
		options.addOptionGroup(optionGroup);
		options.addOption("h", "help", false, "Print this usage information");

		//@formatter:on
		// 解析命令输入
		try {
			CommandLine commandLine = commandLineParser.parse(options, args);
			if (commandLine.hasOption('h')) {
				System.out.println();
				printHelp(options);
				System.exit(0);
			}
			if (commandLine.hasOption('i')) {
				SegmentCommandLine.inputDirectoryPath = commandLine.getOptionValue("i");
			}
			if (commandLine.hasOption('o')) {
				SegmentCommandLine.outputDirectoryPath = commandLine.getOptionValue("o");
			}
			if (commandLine.hasOption("oi")) {
				SegmentCommandLine.oldInputDirectoryPath = commandLine.getOptionValue("oi");
			}
			if (commandLine.hasOption("c")) {
				SegmentCommandLine.inputFileCharset = commandLine.getOptionValue("c", "gbk");
			}
			if (commandLine.hasOption("enw")) {
				String extractNewWordString = commandLine.getOptionValue("enw", "false");
				if (extractNewWordString.equalsIgnoreCase("true")) {
					SegmentCommandLine.extractNewWord = true;
				} else if (extractNewWordString.equalsIgnoreCase("false")) {
					SegmentCommandLine.extractNewWord = false;
				}
			}
			if (commandLine.hasOption("nwof")) {
				SegmentCommandLine.newWordOutputFilePath = commandLine.getOptionValue("nwof");
			}
		} catch (Exception e) {
			System.err.println(e.getMessage());
			SegmentCommandLine.printHelp(options);
			System.exit(1);
		}

		// 参数校验
		if (StringUtils.isBlank(SegmentCommandLine.inputDirectoryPath)
				|| StringUtils.isBlank(SegmentCommandLine.inputFileCharset)
				|| StringUtils.isBlank(SegmentCommandLine.outputDirectoryPath)
				|| StringUtils.isBlank(SegmentCommandLine.oldInputDirectoryPath)) {
			System.err
					.println("Input arguments [inputDirectoryPath, inputFileCharset, outputDirectoryPath, oldInputDirectoryPath] can't null.");
			SegmentCommandLine.printHelp(options);
			System.exit(1);
		}
		if (BooleanUtils.isTrue(SegmentCommandLine.extractNewWord)) {
			if (StringUtils.isBlank(SegmentCommandLine.newWordOutputFilePath)) {
				System.err.println("Input arguments [newWordOutputFilePath] can't null.");
				SegmentCommandLine.printHelp(options);
				System.exit(1);
			}
		} else {
			SegmentCommandLine.extractNewWord = false;
		}

		// 指定输入文件的后缀名
		SegmentCommandLine.inputFileExtensionSet.add("txt");

		// 指定词性
		SegmentCommandLine.posSet.add("n"); // 名称
		SegmentCommandLine.posSet.add("a"); // 形容词

		logger.info("Process starting...");
		/**
		 * 分词准备工作
		 */
		if (SegmentCommandLine.inputFileCharset.equalsIgnoreCase("gbk")) {
			SegmentCommandLine.inputFileCharsetEnum = CharsetEnum.GBK;
		} else if (SegmentCommandLine.inputFileCharset.equalsIgnoreCase("utf8")) {
			SegmentCommandLine.inputFileCharsetEnum = CharsetEnum.UTF8;
		} else {
			logger.info("Unknown input arguments [inputFileCharset], [inputFileCharset] is gbk or utf8.");
			System.exit(1);
		}

		// 准备目录
		File inputDirectory = getInputDirectory();
		File outputDirectory = getOuputDirectory();
		File oldInputDirectory = getOldInputDirectory();
		File newWordOutputFile = getNewWordFile();

		logger.info("Input directory is [ {} ].", inputDirectory.getAbsolutePath());
		logger.info("Output directory is [ {} ].", outputDirectory.getAbsolutePath());
		logger.info("Old input directory is [ {} ].", oldInputDirectory.getAbsolutePath());
		logger.info("Input file charset is [ {} ].", SegmentCommandLine.inputFileCharset);
		if (BooleanUtils.isTrue(SegmentCommandLine.extractNewWord)) {
			logger.info("New word output file is [ {} ].", newWordOutputFile.getAbsolutePath());
		}

		// 加载分词器
		FileSegmentUtil.initializeSegment();

		/**
		 * 将文件夹中的文件分词
		 */
		File[] inputFiles = inputDirectory.listFiles(new ExtensionFileFilter(SegmentCommandLine.inputFileExtensionSet));
		for (File inputFile : inputFiles) {
			FileSegmentUtil.execute(inputFile, oldInputDirectory, outputDirectory, SegmentCommandLine.extractNewWord,
					newWordOutputFile);
		}

		/**
		 * 启动监控进程，监控输入目录的文件变化，调用分词
		 */
		logger.info("Starting file monitor.");
		FileAlterationObserver observer = new FileAlterationObserver(inputDirectory, new ExtensionFileFilter(
				SegmentCommandLine.inputFileExtensionSet));
		FileCreateListenerAdaptor listener = new FileCreateListenerAdaptor(oldInputDirectory, outputDirectory,
				SegmentCommandLine.extractNewWord, newWordOutputFile);
		observer.addListener(listener);
		FileAlterationMonitor fileMonitor = new FileAlterationMonitor(SegmentCommandLine.interval,
				new FileAlterationObserver[] { observer });
		try {
			// 启动开始监听
			fileMonitor.start();
			logger.info("Success to start file monitor.");
		} catch (Exception e) {
			logger.error("Faild to start file monitor. Error message is {}.", e.getMessage());
			System.exit(1);
		}

	}

	/**
	 * 打印命令行使用帮助
	 * 
	 * @param options
	 */
	private static void printHelp(Options options) {
		HelpFormatter helpFormatter = new HelpFormatter();
		helpFormatter.setWidth(SegmentCommandLine.cliWidth);
		helpFormatter.printHelp(SegmentCommandLine.APP_NAME, HEADER, options, FOOTER, true);
	}

	/**
	 * 获得输入目录
	 * 
	 * @return
	 */
	private static File getInputDirectory() {
		File inputDirectory = new File(SegmentCommandLine.inputDirectoryPath);
		if (inputDirectory.exists()) {
			// 输入目录存在，判断是不是一个目录
			if (!inputDirectory.isDirectory()) {
				logger.error("The \"inputDirectory\" is not a directory.");
				System.exit(1);
			}
		} else {
			// 输出目录不存在
			logger.error("The \"inputDirectory\" is not exist.");
			System.exit(1);
		}
		return inputDirectory;
	}

	/**
	 * 获得输出目录
	 * 
	 * @return
	 */
	private static File getOuputDirectory() {
		File outputDirectory = new File(SegmentCommandLine.outputDirectoryPath);
		if (outputDirectory.exists()) {
			// 输出目录存在，判断是不是一个目录
			if (!outputDirectory.isDirectory()) {
				logger.error("The \"outputDirectory\" is not a directory.");
				System.exit(1);
			}
		} else {
			// 输出目录不存在，创建该目录
			if (!outputDirectory.mkdirs()) {
				logger.error("Create \"outputDirectory\" faild.");
				System.exit(1);
			}
		}
		return outputDirectory;
	}

	/**
	 * 获得处理过的文件的输出目录
	 * 
	 * @return
	 */
	private static File getOldInputDirectory() {
		File oldInputDirectory = new File(SegmentCommandLine.oldInputDirectoryPath);
		if (oldInputDirectory.exists()) {
			// 处理过的文件的输出目录存在，判断是不是一个目录
			if (!oldInputDirectory.isDirectory()) {
				logger.error("The \"oldInputDirectory\" is not a directory.");
				System.exit(1);
			}
		} else {
			// 处理过的文件的输出目录不存在，创建该目录
			if (!oldInputDirectory.mkdirs()) {
				logger.error("Create \"oldInputDirectory\" faild.");
				System.exit(1);
			}
		}
		return oldInputDirectory;
	}

	private static File getNewWordFile() {
		if (SegmentCommandLine.extractNewWord) {
			File newWordOutputFile = new File(SegmentCommandLine.newWordOutputFilePath);
			if (newWordOutputFile.exists()) {
				// 输入文件存在，判断是不是一个文件
				if (!newWordOutputFile.isFile()) {
					logger.error("The \"newWordOutputFile\" is not a file.");
					System.exit(1);
				}
			} else {
				// 输出文件不存在，创建该文件
				try {
					newWordOutputFile.createNewFile();
				} catch (IOException e) {
					logger.error("Create \"newWordOutputFile\" faild.");
					System.exit(1);
				}

			}
			return newWordOutputFile;
		} else {
			return null;
		}
	}
}
