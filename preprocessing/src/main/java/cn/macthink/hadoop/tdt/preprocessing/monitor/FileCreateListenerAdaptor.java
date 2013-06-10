/**
 * Project:hadoop-tdt-preprocessing
 * File Created at 2013-4-27
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.preprocessing.monitor;

import java.io.File;

import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;

import cn.macthink.hadoop.tdt.preprocessing.IndexFileExtractor;

/**
 * FileCreateListenerAdaptor
 * 
 * @author Macthink
 */
public class FileCreateListenerAdaptor extends FileAlterationListenerAdaptor {

	private File oldInputDirectory;
	private File outputDirectory;

	/**
	 * 新建文件监听适配器
	 * 
	 * @param oldInputDirectory
	 * @param outputDirectory
	 */
	public FileCreateListenerAdaptor(File oldInputDirectory, File outputDirectory) {
		super();
		this.oldInputDirectory = oldInputDirectory;
		this.outputDirectory = outputDirectory;
	}

	@Override
	public void onFileCreate(File inputFile) {
		super.onFileCreate(inputFile);
		IndexFileExtractor.extract(inputFile, this.oldInputDirectory, this.outputDirectory);
	}

	public File getOldInputDirectory() {
		return oldInputDirectory;
	}

	public void setOldInputDirectory(File oldInputDirectory) {
		this.oldInputDirectory = oldInputDirectory;
	}

	public File getOutputDirectory() {
		return outputDirectory;
	}

	public void setOutputDirectory(File outputDirectory) {
		this.outputDirectory = outputDirectory;
	}

}
