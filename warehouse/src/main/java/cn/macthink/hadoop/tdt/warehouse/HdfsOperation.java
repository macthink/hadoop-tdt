/**
 * Project:hadoop-tdt-warehouse
 * File Created at 2013-4-29
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.warehouse;

import java.io.File;
import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HdfsOperation
 * 
 * @author Macthink
 */
public class HdfsOperation {

	private static Logger logger = LoggerFactory.getLogger(HdfsOperation.class);

	/**
	 * 列出HDFS上给定目录的文件列表
	 * 
	 * @param conf
	 * @param listPathString
	 */
	public static void list(Configuration conf, String listPathString) {
		// 参数检测
		if (conf == null || StringUtils.isBlank(conf.get(FileSystem.FS_DEFAULT_NAME_KEY))
				|| StringUtils.isBlank(listPathString)) {
			return;
		}
		// 获得文件系统
		FileSystem fileSystem = getFileSystem(conf);
		if (fileSystem == null) {
			return;
		}
		// 列出文件列表
		String fsDefaultName = conf.get(FileSystem.FS_DEFAULT_NAME_KEY);
		Path listPath = new Path(fsDefaultName, listPathString);
		FileStatus fileStatuses[] = null;
		try {
			fileStatuses = fileSystem.listStatus(listPath);
			for (FileStatus fileStatus : fileStatuses) {
				logger.info(fileStatus.getPath().toString());
			}
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}

	/**
	 * 从HDFS下载文件
	 * 
	 * @param conf
	 * @param hdfsFilePathString
	 * @param localFilePathString
	 * @return
	 */
	public static boolean get(Configuration conf, String hdfsFilePathString, String localFilePathString) {
		return get(conf, hdfsFilePathString, localFilePathString, false);
	}

	/**
	 * 从HDFS下载文件
	 * 
	 * @param conf
	 * @param hdfsFilePathString
	 * @param localFilePathString
	 * @param deleteHdfsFile
	 * @return
	 */
	public static boolean get(Configuration conf, String hdfsFilePathString, String localFilePathString,
			boolean deleteHdfsFile) {
		// 参数检测
		if (conf == null || StringUtils.isBlank(conf.get(FileSystem.FS_DEFAULT_NAME_KEY))
				|| StringUtils.isBlank(hdfsFilePathString) || StringUtils.isBlank(localFilePathString)) {
			return false;
		}
		// 获得文件系统
		FileSystem fileSystem = getFileSystem(conf);
		if (fileSystem == null) {
			return false;
		}
		// 下载文件
		Path localFilePath = new Path(localFilePathString);
		String fsDefaultName = conf.get(FileSystem.FS_DEFAULT_NAME_KEY);
		Path hdfsFilePath = new Path(fsDefaultName, hdfsFilePathString);
		logger.info("From [ {} ] to [ {} ].", hdfsFilePath.toString(), localFilePath.toString());
		try {
			fileSystem.copyToLocalFile(deleteHdfsFile, hdfsFilePath, localFilePath);
			return true;
		} catch (IOException e) {
			logger.error(e.getMessage());
			return false;
		}
	}

	/**
	 * 上传文件到HDFS
	 * 
	 * @param conf
	 * @param localFile
	 * @param hdfsDirectory
	 * @return
	 */
	public static boolean put(Configuration conf, File localFile, String hdfsDirectory) {
		return put(conf, localFile, hdfsDirectory, false, true);
	}

	/**
	 * 上传文件到HDFS
	 * 
	 * @param conf
	 * @param localFile
	 * @param hdfsPath
	 * @param deleteLocalFile
	 * @param overwrite
	 */
	public static boolean put(Configuration conf, File localFile, String hdfsDirectory, boolean deleteLocalFile,
			boolean overwrite) {
		// 参数检测
		if (conf == null || StringUtils.isBlank(conf.get(FileSystem.FS_DEFAULT_NAME_KEY)) || localFile == null
				|| !localFile.exists() || StringUtils.isBlank(hdfsDirectory)) {
			return false;
		}
		// 获得文件系统
		FileSystem fileSystem = getFileSystem(conf);
		if (fileSystem == null) {
			return false;
		}
		// 上传文件
		Path localFilePath = new Path(localFile.getAbsolutePath());
		String fsDefaultName = conf.get(FileSystem.FS_DEFAULT_NAME_KEY);
		Path hdfsDirectoryPath = new Path(fsDefaultName, hdfsDirectory);
		logger.info("From [ {} ] to [ {} ].", localFilePath.toString(), hdfsDirectoryPath.toString());
		try {
			if (fileSystem.mkdirs(hdfsDirectoryPath)) {
				fileSystem.copyFromLocalFile(deleteLocalFile, overwrite, localFilePath, hdfsDirectoryPath);
			}
			return true;
		} catch (IOException e) {
			logger.error(e.getMessage());
			return false;
		}
	}

	/**
	 * 获得文件系统
	 * 
	 * @param conf
	 * @return
	 */
	private static FileSystem getFileSystem(Configuration conf) {
		// 参数检测
		if (conf == null) {
			return null;
		}

		// 获得文件系统
		FileSystem fileSystem = null;
		try {
			fileSystem = FileSystem.get(conf);
			return fileSystem;
		} catch (IOException e) {
			logger.error(e.getMessage());
			return null;
		}
	}
}
