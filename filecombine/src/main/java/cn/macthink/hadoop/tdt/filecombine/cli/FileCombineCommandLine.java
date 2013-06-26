/**
 * Project:hadoop-tdt-preprocessing
 * File Created at 2013-4-22
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.filecombine.cli;

import java.io.File;
import java.io.FileFilter;
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
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.macthink.hadoop.tdt.filecombine.FileCombine;
import cn.macthink.hadoop.tdt.filecombine.monitor.ExtensionFileFilter;
import cn.macthink.hadoop.tdt.filecombine.monitor.FileCreateListenerAdaptor;

/**
 * PreprocessingCommandLine：文件夹文件批预处理
 * 
 * @author Macthink
 */
public class FileCombineCommandLine {

	private static Logger logger = LoggerFactory.getLogger(FileCombineCommandLine.class);

	// 程序信息
	private static final String APP_NAME = "filecombine";
	private static final String HEADER = "FileCombine - A auto file combine tool for TDT, Copyright 2013 Macthink.";
	private static final String FOOTER = "For more information, see: macthink.cn. Bug Reports to <macthink@hqu.edu.cn>";

	// 需要的输入参数
	public static String inputDirectoryPath = null;
	public static String outputFilePath = null;
	public static String inputFileCharset = null;
	public static Boolean isRecursive = false;

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
		// input
		OptionGroup optionGroup = new OptionGroup();
		Option iOption = OptionBuilder.withLongOpt("input")
				.hasArg(true)
				.withArgName("inputDirectoryPath")
				.withDescription("Absolute path of input directory")
				.isRequired(true)
				.create("i");
		optionGroup.addOption(iOption);
		options.addOptionGroup(optionGroup);
		// output
		optionGroup = new OptionGroup();
		Option oOption = OptionBuilder.withLongOpt("output")
				.hasArg(true)
				.withArgName("outputFilePath")
				.withDescription("Absolute path of output file")
				.isRequired(true)
				.create("o");
		optionGroup.addOption(oOption);
		options.addOptionGroup(optionGroup);
		// charset
		optionGroup = new OptionGroup();
		Option cOption = OptionBuilder.withLongOpt("charset")
				.hasArg(true)
				.withArgName("inputFileCharset")
				.withDescription("Charset of the input files")
				.create("c");
		optionGroup.addOption(cOption);
		options.addOptionGroup(optionGroup);
		// recursive
		optionGroup = new OptionGroup();
		Option rOption = OptionBuilder.withLongOpt("recursive")
				.hasArg(true)
				.withArgName("isRecursive")
				.withDescription("Process subdirectories recursively")
				.create("r");
		optionGroup.addOption(rOption);
		options.addOptionGroup(optionGroup);
		// help
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
				FileCombineCommandLine.inputDirectoryPath = commandLine.getOptionValue("i");
			}
			if (commandLine.hasOption('o')) {
				FileCombineCommandLine.outputFilePath = commandLine.getOptionValue("o");
			}
			if (commandLine.hasOption("c")) {
				FileCombineCommandLine.inputFileCharset = commandLine.getOptionValue("c", "gbk");
			}
			if (commandLine.hasOption("r")) {
				FileCombineCommandLine.isRecursive = Boolean.parseBoolean(commandLine.getOptionValue("r", "false"));
			}
		} catch (Exception e) {
			System.err.println(e.getMessage());
			FileCombineCommandLine.printHelp(options);
			System.exit(1);
		}

		// 参数校验
		if (StringUtils.isBlank(FileCombineCommandLine.inputDirectoryPath)
				|| StringUtils.isBlank(FileCombineCommandLine.inputFileCharset)
				|| StringUtils.isBlank(FileCombineCommandLine.outputFilePath)) {
			System.err.println("Input arguments [inputDirectoryPath, inputFileCharset, outputFilePath] can't be null.");
			FileCombineCommandLine.printHelp(options);
			System.exit(1);
		}
		if (!FileCombineCommandLine.inputFileCharset.equalsIgnoreCase("gbk")
				&& !FileCombineCommandLine.inputFileCharset.equalsIgnoreCase("utf8")) {
			logger.info("Unknown input arguments [inputFileCharset], [inputFileCharset] is gbk or utf8.");
			System.exit(1);
		}

		// 指定输入文件的后缀名
		FileCombineCommandLine.inputFileExtensionSet.add("txt");

		logger.info("Process starting...");

		// 准备目录
		File inputDirectory = getInputDirectory();
		File outputFile = getOuputFile();

		logger.info("Input directory is [ {} ].", inputDirectory.getAbsolutePath());
		logger.info("Output file is [ {} ].", outputFile.getAbsolutePath());
		logger.info("Input file charset is [ {} ].", FileCombineCommandLine.inputFileCharset);

		/**
		 * 处理文件夹中的文件
		 */
		if (!FileCombineCommandLine.isRecursive) {
			File[] inputFiles = inputDirectory.listFiles(new ExtensionFileFilter(
					FileCombineCommandLine.inputFileExtensionSet));
			for (File inputFile : inputFiles) {
				if (inputFile.isFile()) {
					FileCombine.combine(inputFile, outputFile);
				}
			}
		} else {
			combineExistFileRecursive(inputDirectory, outputFile);
		}

		/**
		 * 启动监控进程，监控输入目录的文件变化，调用处理器
		 */
		logger.info("Starting file monitor.");
		// TODO:递归不能动态监听文件夹变化
		FileAlterationObserver observer = new FileAlterationObserver(inputDirectory, new ExtensionFileFilter(
				FileCombineCommandLine.inputFileExtensionSet));
		FileCreateListenerAdaptor listener = new FileCreateListenerAdaptor(outputFile);
		observer.addListener(listener);
		FileAlterationMonitor fileMonitor = new FileAlterationMonitor(FileCombineCommandLine.interval,
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
		helpFormatter.setWidth(FileCombineCommandLine.cliWidth);
		helpFormatter.printHelp(FileCombineCommandLine.APP_NAME, HEADER, options, FOOTER, true);
	}

	/**
	 * 获得输入目录
	 * 
	 * @return
	 */
	private static File getInputDirectory() {
		File inputDirectory = new File(FileCombineCommandLine.inputDirectoryPath);
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
	 * 递归处理文件夹中的文件
	 * 
	 * @return
	 */
	public static void combineExistFileRecursive(File inputDirectory, File outputFile) {
		// 遍历该目录下的子目录
		File[] subDirectories = inputDirectory.listFiles(new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				if (pathname.isDirectory()) {
					return true;
				} else {
					return false;
				}
			}
		});
		for (File directory : subDirectories) {
			combineExistFileRecursive(directory, outputFile);
		}

		// 遍历该目录下的文件
		File[] subFiles = inputDirectory
				.listFiles(new ExtensionFileFilter(FileCombineCommandLine.inputFileExtensionSet));
		for (File file : subFiles) {
			FileCombine.combine(file, outputFile);
		}
	}

	/**
	 * 获得输出文件路径
	 * 
	 * @return
	 * @throws IOException
	 */
	private static File getOuputFile() {
		File outputFile = new File(FileCombineCommandLine.outputFilePath);
		if (outputFile.exists()) {
			outputFile.delete();
		}
		try {
			if (!outputFile.createNewFile()) {
				logger.error("Create \"outputFile\" faild.");
				System.exit(1);
			}
		} catch (IOException e) {
			logger.error("Create \"outputFile\" faild. Error message is {}.", e.getMessage());
		}
		return outputFile;
	}

}
