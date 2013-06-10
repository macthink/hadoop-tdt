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
import java.io.FileFilter;
import java.util.Set;

/**
 * ExtensionFileFilter：扩展名文件过滤器（文件需要有扩展名，且需要的保留的扩展名通过extensionSet设置）
 * 
 * @author Macthink
 */
public class ExtensionFileFilter implements FileFilter {

	// 保留的扩展名
	private Set<String> extensionSet;

	/**
	 * 扩展名文件过滤器
	 * 
	 * @param extensionSet
	 *            保留的扩展名
	 */
	public ExtensionFileFilter(Set<String> extensionSet) {
		super();
		this.extensionSet = extensionSet;
	}

	@Override
	public boolean accept(File file) {
		if (file.isDirectory()) {
			return false;
		}
		String fileName = file.getName();
		int index = fileName.lastIndexOf(".");
		if (index == -1) {
			return false;
		} else if (index == fileName.length() - 1) {
			return false;
		} else {
			if (this.extensionSet == null) {
				return true;
			} else {
				return this.extensionSet.contains(fileName.substring(index + 1));
			}
		}
	}

	public Set<String> getExtensionSet() {
		return extensionSet;
	}

	public void setExtensionSet(Set<String> extensionSet) {
		this.extensionSet = extensionSet;
	}

}
