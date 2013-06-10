/**
 * Project:hadoop-tdt-segment
 * File Created at 2013-4-22
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.warehouse.cli;

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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.macthink.hadoop.tdt.warehouse.HdfsOperation;
import cn.macthink.hadoop.tdt.warehouse.monitor.ExtensionFileFilter;
import cn.macthink.hadoop.tdt.warehouse.monitor.FileCreateListenerAdaptor;

/**
 * FolderSegment：文件夹文件批分词
 * 
 * @author Macthink
 */
public class HdfsOperationCommandLine {

	private static Logger logger = LoggerFactory.getLogger(HdfsOperationCommandLine.class);

	// 程序信息
	private static final String APP_NAME = "warehouse";
	private static final String HEADER = "Warehouse - A auto warehouse tool for TDT, Copyright 2013 Macthink.";
	private static final String FOOTER = "For more information, see: macthink.cn. Bug Reports to <macthink@hqu.edu.cn>";

	// 需要的输入参数
	public static String fsDefaultName = null;
	public static String inputDirectoryPath = null;
	public static String hdfsFilePath = null;
	public static Boolean overwrite = null;
	public static Boolean deleteLocalFile = null;

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
		//fsOption
		OptionGroup optionGroup = new OptionGroup();
		Option fsOption = OptionBuilder.withLongOpt("fsname")
				.hasArg(true)
				.withArgName("fsDefaultName")
				.withDescription("The fs default name of HDFS")
				.isRequired(true)
				.create("fsn");
		optionGroup.addOption(fsOption);
		options.addOptionGroup(optionGroup);
		//iOption
		optionGroup = new OptionGroup();
		Option iOption = OptionBuilder.withLongOpt("input")
				.hasArg(true)
				.withArgName("inputDirectoryPath")
				.withDescription("Absolute path of input directory")
				.isRequired(true)
				.create("i");
		optionGroup.addOption(iOption);
		options.addOptionGroup(optionGroup);
		//hfpOption
		optionGroup = new OptionGroup();
		Option hfpOption = OptionBuilder.withLongOpt("hdfsPath")
				.hasArg(true)
				.withArgName("hdfsFilePath")
				.withDescription("HDFS path of output directory")
				.isRequired(true)
				.create("hfp");
		optionGroup.addOption(hfpOption);
		options.addOptionGroup(optionGroup);
		//owOption
		optionGroup = new OptionGroup();
		Option owOption = OptionBuilder.withLongOpt("overwrite")
				.hasArg(true)
				.withArgName("true/false")
				.withDescription("Overwrite file if exist file in HDFS")
				.isRequired(true)
				.create("ow");
		optionGroup.addOption(owOption);	
		options.addOptionGroup(optionGroup);
		//dlfOption 
		optionGroup = new OptionGroup();
		Option dlfOption = OptionBuilder.withLongOpt("deleteLocalFile")
				.hasArg(true)
				.withArgName("true/false")
				.withDescription("Delete local file after upload HDFS")
				.isRequired(true)
				.create("dlf");
		optionGroup.addOption(dlfOption);	
		options.addOptionGroup(optionGroup);
		//h
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
			if (commandLine.hasOption("fsn")) {
				HdfsOperationCommandLine.fsDefaultName = commandLine.getOptionValue("fsn");
			}
			if (commandLine.hasOption('i')) {
				HdfsOperationCommandLine.inputDirectoryPath = commandLine.getOptionValue("i");
			}
			if (commandLine.hasOption("hfp")) {
				HdfsOperationCommandLine.hdfsFilePath = commandLine.getOptionValue("hfp");
			}
			if (commandLine.hasOption("ow")) {
				String overwriteString = commandLine.getOptionValue("ow", "true");
				if (overwriteString.equalsIgnoreCase("true")) {
					HdfsOperationCommandLine.overwrite = true;
				} else if (overwriteString.equalsIgnoreCase("false")) {
					HdfsOperationCommandLine.overwrite = false;
				}
			}
			if (commandLine.hasOption("dlf")) {
				String deleteLocalFileString = commandLine.getOptionValue("dlf", "true");
				if (deleteLocalFileString.equalsIgnoreCase("true")) {
					HdfsOperationCommandLine.deleteLocalFile = true;
				} else if (deleteLocalFileString.equalsIgnoreCase("false")) {
					HdfsOperationCommandLine.deleteLocalFile = false;
				}
			}
		} catch (Exception e) {
			System.err.println(e.getMessage());
			HdfsOperationCommandLine.printHelp(options);
			System.exit(1);
		}

		// 参数校验
		if (StringUtils.isBlank(HdfsOperationCommandLine.fsDefaultName)
				|| StringUtils.isBlank(HdfsOperationCommandLine.inputDirectoryPath)
				|| StringUtils.isBlank(HdfsOperationCommandLine.hdfsFilePath)) {
			System.err.println("Input arguments [fsDefaultName, inputDirectoryPath, hdfsFilePath] can't be null.");
			HdfsOperationCommandLine.printHelp(options);
			System.exit(1);
		}

		// 指定输入文件的后缀名
		HdfsOperationCommandLine.inputFileExtensionSet.add("txt");

		logger.info("Process starting...");

		// 准备目录
		File inputDirectory = getInputDirectory();

		logger.info("The fs default name of HDFS is [ {} ].", HdfsOperationCommandLine.fsDefaultName);
		logger.info("Input directory is [ {} ].", inputDirectory.getAbsolutePath());
		logger.info("HDFS path of output directory is [ {} ].", HdfsOperationCommandLine.hdfsFilePath);
		if (HdfsOperationCommandLine.overwrite != null) {
			logger.info("Overwrite file if exist file in HDFS: [ {} ].", HdfsOperationCommandLine.overwrite);
		}
		if (HdfsOperationCommandLine.deleteLocalFile != null) {
			logger.info("Delete local file after upload HDFS: [ {} ].", HdfsOperationCommandLine.deleteLocalFile);
		}

		// 准备参数
		Configuration conf = new Configuration();
		conf.set(FileSystem.FS_DEFAULT_NAME_KEY, HdfsOperationCommandLine.fsDefaultName);

		/**
		 * 将文件夹中的文件入库
		 */
		File[] inputFiles = inputDirectory.listFiles(new ExtensionFileFilter(
				HdfsOperationCommandLine.inputFileExtensionSet));
		for (File inputFile : inputFiles) {
			HdfsOperation.put(conf, inputFile, HdfsOperationCommandLine.hdfsFilePath,
					HdfsOperationCommandLine.deleteLocalFile, HdfsOperationCommandLine.overwrite);
		}

		/**
		 * 启动监控进程，监控输入目录的文件变化，调用分词
		 */
		logger.info("Starting file monitor.");
		FileAlterationObserver observer = new FileAlterationObserver(inputDirectory, new ExtensionFileFilter(
				HdfsOperationCommandLine.inputFileExtensionSet));
		FileCreateListenerAdaptor listener = new FileCreateListenerAdaptor(conf);
		observer.addListener(listener);
		FileAlterationMonitor fileMonitor = new FileAlterationMonitor(HdfsOperationCommandLine.interval,
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
		helpFormatter.setWidth(HdfsOperationCommandLine.cliWidth);
		helpFormatter.printHelp(HdfsOperationCommandLine.APP_NAME, HEADER, options, FOOTER, true);
	}

	/**
	 * 获得输入目录
	 * 
	 * @return
	 */
	private static File getInputDirectory() {
		File inputDirectory = new File(HdfsOperationCommandLine.inputDirectoryPath);
		if (inputDirectory.exists()) {
			// 输入目录存在，判断是不是一个目录
			if (!inputDirectory.isDirectory()) {
				logger.error("The \"inputDirectory\" is not a directory.");
				System.exit(1);
			}
		} else {
			// 输出目录不存在，创建该目录
			logger.error("The \"inputDirectory\" is not exist.");
			System.exit(1);
		}
		return inputDirectory;
	}

}
