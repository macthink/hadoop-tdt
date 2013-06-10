/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-5-5
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.util.compound;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableName;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * CompoundKeyValuePair：复合键值对
 * 
 * @author Macthink
 * @param <K>
 * @param <V>
 */
@SuppressWarnings("rawtypes")
public class CompoundKeyValuePair<K extends WritableComparable, V extends WritableComparable> implements
		WritableComparable<CompoundKeyValuePair<K, V>> {

	private static Configuration conf = new Configuration();

	private K first;
	private V second;

	private Class<K> firstClass;
	private Class<V> secondClass;

	/**
	 * CompoundKeyValuePair
	 */
	public CompoundKeyValuePair() {
		super();
	}

	/**
	 * @param first
	 * @param second
	 */
	public CompoundKeyValuePair(K first, V second) {
		super();
		this.first = first;
		this.second = second;
		this.firstClass = ReflectionUtils.getClass(first);
		this.secondClass = ReflectionUtils.getClass(second);
	}

	/**
	 * CompoundKeyValuePair
	 * 
	 * @param firstClass
	 * @param secondClass
	 */
	public CompoundKeyValuePair(Class<K> firstClass, Class<V> secondClass) {
		super();
		if (firstClass == null || secondClass == null) {
			throw new IllegalArgumentException("Argument is null!");
		}
		this.firstClass = firstClass;
		this.secondClass = secondClass;
		first = ReflectionUtils.newInstance(firstClass, conf);
		second = ReflectionUtils.newInstance(secondClass, conf);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(firstClass.getName());
		out.writeUTF(secondClass.getName());
		first.write(out);
		second.write(out);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readFields(DataInput in) throws IOException {
		String firstClassName = in.readUTF();
		String secondClassName = in.readUTF();
		firstClass = (Class<K>) WritableName.getClass(firstClassName, conf);
		secondClass = (Class<V>) WritableName.getClass(secondClassName, conf);
		first = ReflectionUtils.newInstance(firstClass, conf);
		second = ReflectionUtils.newInstance(secondClass, conf);
		first.readFields(in);
		second.readFields(in);
	}

	@SuppressWarnings("unchecked")
	@Override
	public int compareTo(CompoundKeyValuePair<K, V> o) {
		CompoundKeyValuePair<K, V> that = (CompoundKeyValuePair<K, V>) o;
		return that.first.compareTo(this.first);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((first == null) ? 0 : first.hashCode());
		return result;
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		CompoundKeyValuePair<K, V> other = (CompoundKeyValuePair<K, V>) obj;
		if (first == null) {
			if (other.first != null) {
				return false;
			}
		} else if (!first.equals(other.first)) {
			return false;
		}
		return true;
	}

	/**
	 * @return the first
	 */
	public K getFirst() {
		return first;
	}

	/**
	 * @return the second
	 */
	public V getSecond() {
		return second;
	}

	/**
	 * @return the firstClass
	 */
	public Class<K> getFirstClass() {
		return firstClass;
	}

	/**
	 * @return the secondClass
	 */
	public Class<V> getSecondClass() {
		return secondClass;
	}

	/**
	 * @param first
	 *            the first to set
	 */
	public void setFirst(K first) {
		this.first = first;
	}

	/**
	 * @param second
	 *            the second to set
	 */
	public void setSecond(V second) {
		this.second = second;
	}

	/**
	 * @param firstClass
	 *            the firstClass to set
	 */
	public void setFirstClass(Class<K> firstClass) {
		this.firstClass = firstClass;
	}

	/**
	 * @param secondClass
	 *            the secondClass to set
	 */
	public void setSecondClass(Class<V> secondClass) {
		this.secondClass = secondClass;
	}

}
