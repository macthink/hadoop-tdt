/**
 * Project:hadoop-tdt-filecombine
 * File Created at 2013-4-27
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.filecombine.monitor;

import java.io.File;

import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;

import cn.macthink.hadoop.tdt.filecombine.FileCombine;

/**
 * FileCreateListenerAdaptor
 * 
 * @author Macthink
 */
public class FileCreateListenerAdaptor extends FileAlterationListenerAdaptor {

	private File outputFile;

	/**
	 * 新建文件监听适配器
	 * 
	 * @param configuration
	 */
	public FileCreateListenerAdaptor(File outputFile) {
		super();
		this.outputFile = outputFile;
	}

	@Override
	public void onFileCreate(File inputFile) {
		super.onFileCreate(inputFile);
		FileCombine.combine(inputFile, outputFile);
	}

	/**
	 * @return the outputFile
	 */
	public File getOutputFile() {
		return outputFile;
	}

	/**
	 * @param outputFile
	 *            the outputFile to set
	 */
	public void setOutputFile(File outputFile) {
		this.outputFile = outputFile;
	}

}
