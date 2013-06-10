/**
 * Project:hadoop-tdt-preprocessing
 * File Created at 2013-4-22
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.preprocessing.cli;

import java.io.File;
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
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.macthink.hadoop.tdt.preprocessing.IndexFileExtractor;
import cn.macthink.hadoop.tdt.preprocessing.monitor.ExtensionFileFilter;
import cn.macthink.hadoop.tdt.preprocessing.monitor.FileCreateListenerAdaptor;

/**
 * PreprocessingCommandLine：文件夹文件批预处理
 * 
 * @author Macthink
 */
public class PreprocessingCommandLine {

	private static Logger logger = LoggerFactory.getLogger(PreprocessingCommandLine.class);

	// 程序信息
	private static final String APP_NAME = "preprocessing";
	private static final String HEADER = "Preprocessing - A auto preprocessing tool for TDT, Copyright 2013 Macthink.";
	private static final String FOOTER = "For more information, see: macthink.cn. Bug Reports to <macthink@hqu.edu.cn>";

	// 需要的输入参数
	public static String inputDirectoryPath = null;
	public static String outputDirectoryPath = null;
	public static String oldInputDirectoryPath = null;
	public static String inputFileCharset = null;

	// 需要处理的输入文件的后缀名列表
	public static Set<String> inputFileExtensionSet = new HashSet<String>();
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
		OptionGroup optionGroup = new OptionGroup();
		Option iOption = OptionBuilder.withLongOpt("input")
				.hasArg(true)
				.withArgName("inputDirectoryPath")
				.withDescription("Absolute path of input directory")
				.isRequired(true)
				.create("i");
		optionGroup.addOption(iOption);
		options.addOptionGroup(optionGroup);
		optionGroup = new OptionGroup();
		Option oOption = OptionBuilder.withLongOpt("output")
				.hasArg(true)
				.withArgName("outputDirectoryPath")
				.withDescription("Absolute path of output directory")
				.isRequired(true)
				.create("o");
		optionGroup.addOption(oOption);
		options.addOptionGroup(optionGroup);
		optionGroup = new OptionGroup();
		Option oiOption = OptionBuilder.withLongOpt("oldInput")
				.hasArg(true)
				.withArgName("oldInputDirectoryPath")
				.withDescription("Absolute path of old output directory")
				.isRequired(true)
				.create("oi");
		optionGroup.addOption(oiOption);	
		options.addOptionGroup(optionGroup);
		optionGroup = new OptionGroup();
		Option cOption = OptionBuilder.withLongOpt("charset")
				.hasArg(true)
				.withArgName("inputFileCharset")
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
				PreprocessingCommandLine.inputDirectoryPath = commandLine.getOptionValue("i");
			}
			if (commandLine.hasOption('o')) {
				PreprocessingCommandLine.outputDirectoryPath = commandLine.getOptionValue("o");
			}
			if (commandLine.hasOption("oi")) {
				PreprocessingCommandLine.oldInputDirectoryPath = commandLine.getOptionValue("oi");
			}
			if (commandLine.hasOption("c")) {
				PreprocessingCommandLine.inputFileCharset = commandLine.getOptionValue("c", "gbk");
			}
		} catch (Exception e) {
			System.err.println(e.getMessage());
			PreprocessingCommandLine.printHelp(options);
			System.exit(1);
		}

		// 参数校验
		if (StringUtils.isBlank(PreprocessingCommandLine.inputDirectoryPath)
				|| StringUtils.isBlank(PreprocessingCommandLine.inputFileCharset)
				|| StringUtils.isBlank(PreprocessingCommandLine.outputDirectoryPath)
				|| StringUtils.isBlank(PreprocessingCommandLine.oldInputDirectoryPath)) {
			System.err
					.println("Input arguments [inputDirectoryPath, inputFileCharset, outputDirectoryPath, oldInputDirectoryPath] can't be null.");
			PreprocessingCommandLine.printHelp(options);
			System.exit(1);
		}
		if (!PreprocessingCommandLine.inputFileCharset.equalsIgnoreCase("gbk")
				&& !PreprocessingCommandLine.inputFileCharset.equalsIgnoreCase("utf8")) {
			logger.info("Unknown input arguments [inputFileCharset], [inputFileCharset] is gbk or utf8.");
			System.exit(1);
		}

		// 指定输入文件的后缀名
		PreprocessingCommandLine.inputFileExtensionSet.add("index");

		logger.info("Process starting...");

		// 准备目录
		File inputDirectory = getInputDirectory();
		File outputDirectory = getOuputDirectory();
		File oldInputDirectory = getOldInputDirectory();

		logger.info("Input directory is [ {} ].", inputDirectory.getAbsolutePath());
		logger.info("Output directory is [ {} ].", outputDirectory.getAbsolutePath());
		logger.info("Old input directory is [ {} ].", oldInputDirectory.getAbsolutePath());
		logger.info("Input file charset is [ {} ].", PreprocessingCommandLine.inputFileCharset);

		/**
		 * 解析文件夹中的.index文件
		 */
		File[] inputFiles = inputDirectory.listFiles(new ExtensionFileFilter(
				PreprocessingCommandLine.inputFileExtensionSet));
		for (File inputFile : inputFiles) {
			IndexFileExtractor.extract(inputFile, oldInputDirectory, outputDirectory);
		}

		/**
		 * 启动监控进程，监控输入目录的文件变化，调用解析器
		 */
		logger.info("Starting file monitor.");
		FileAlterationObserver observer = new FileAlterationObserver(inputDirectory, new ExtensionFileFilter(
				PreprocessingCommandLine.inputFileExtensionSet));
		FileCreateListenerAdaptor listener = new FileCreateListenerAdaptor(oldInputDirectory, outputDirectory);
		observer.addListener(listener);
		FileAlterationMonitor fileMonitor = new FileAlterationMonitor(PreprocessingCommandLine.interval,
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
		helpFormatter.setWidth(PreprocessingCommandLine.cliWidth);
		helpFormatter.printHelp(PreprocessingCommandLine.APP_NAME, HEADER, options, FOOTER, true);
	}

	/**
	 * 获得输入目录
	 * 
	 * @return
	 */
	private static File getInputDirectory() {
		File inputDirectory = new File(PreprocessingCommandLine.inputDirectoryPath);
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
		File outputDirectory = new File(PreprocessingCommandLine.outputDirectoryPath);
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
		File oldInputDirectory = new File(PreprocessingCommandLine.oldInputDirectoryPath);
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

}
