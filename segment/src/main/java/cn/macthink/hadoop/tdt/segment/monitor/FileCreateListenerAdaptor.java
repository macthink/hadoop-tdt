/**
 * Project:hadoop-tdt-segment
 * File Created at 2013-4-27
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.segment.monitor;

import java.io.File;

import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;

import cn.macthink.hadoop.tdt.segment.FileSegmentUtil;

/**
 * FileCreateListenerAdaptor
 * 
 * @author Macthink
 */
public class FileCreateListenerAdaptor extends FileAlterationListenerAdaptor {

	private File oldInputDirectory;
	private File outputDirectory;
	private boolean extractNewWord;
	private File newWordFile;

	/**
	 * 新建文件监听适配器
	 * 
	 * @param oldInputDirectory
	 * @param outputDirectory
	 * @param extractNewWord
	 * @param newWordFile
	 */
	public FileCreateListenerAdaptor(File oldInputDirectory, File outputDirectory, boolean extractNewWord,
			File newWordFile) {
		super();
		this.oldInputDirectory = oldInputDirectory;
		this.outputDirectory = outputDirectory;
		this.extractNewWord = extractNewWord;
		this.newWordFile = newWordFile;
	}

	@Override
	public void onFileCreate(File inputFile) {
		super.onFileCreate(inputFile);
		FileSegmentUtil.execute(inputFile, this.oldInputDirectory, this.outputDirectory, this.extractNewWord,
				this.newWordFile);
	}

	/**
	 * @return the oldInputDirectory
	 */
	public File getOldInputDirectory() {
		return oldInputDirectory;
	}

	/**
	 * @return the outputDirectory
	 */
	public File getOutputDirectory() {
		return outputDirectory;
	}

	/**
	 * @return the extractNewWord
	 */
	public boolean isExtractNewWord() {
		return extractNewWord;
	}

	/**
	 * @return the newWordFile
	 */
	public File getNewWordFile() {
		return newWordFile;
	}

	/**
	 * @param oldInputDirectory
	 *            the oldInputDirectory to set
	 */
	public void setOldInputDirectory(File oldInputDirectory) {
		this.oldInputDirectory = oldInputDirectory;
	}

	/**
	 * @param outputDirectory
	 *            the outputDirectory to set
	 */
	public void setOutputDirectory(File outputDirectory) {
		this.outputDirectory = outputDirectory;
	}

	/**
	 * @param extractNewWord
	 *            the extractNewWord to set
	 */
	public void setExtractNewWord(boolean extractNewWord) {
		this.extractNewWord = extractNewWord;
	}

	/**
	 * @param newWordFile
	 *            the newWordFile to set
	 */
	public void setNewWordFile(File newWordFile) {
		this.newWordFile = newWordFile;
	}

}
