/**
 * Project:hadoop-tdt-segment
 * File Created at 2013-4-27
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.warehouse.monitor;

import java.io.File;

import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.hadoop.conf.Configuration;

import cn.macthink.hadoop.tdt.warehouse.HdfsOperation;
import cn.macthink.hadoop.tdt.warehouse.cli.HdfsOperationCommandLine;

/**
 * FileCreateListenerAdaptor
 * 
 * @author Macthink
 */
public class FileCreateListenerAdaptor extends FileAlterationListenerAdaptor {

	private Configuration configuration;

	/**
	 * 新建文件监听适配器
	 * 
	 * @param configuration
	 */
	public FileCreateListenerAdaptor(Configuration configuration) {
		super();
		this.configuration = configuration;
	}

	@Override
	public void onFileCreate(File inputFile) {
		HdfsOperation.put(this.configuration, inputFile, HdfsOperationCommandLine.hdfsFilePath,
				HdfsOperationCommandLine.deleteLocalFile, HdfsOperationCommandLine.overwrite);
	}

	/**
	 * @return the configuration
	 */
	public Configuration getConfiguration() {
		return configuration;
	}

	/**
	 * @param configuration
	 *            the configuration to set
	 */
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

}
