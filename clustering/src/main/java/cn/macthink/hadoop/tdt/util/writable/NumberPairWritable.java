/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-6-5
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.util.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * NumberPairWritable
 * 
 * @author Macthink
 */
public class NumberPairWritable<K1 extends WritableComparable<K1>, K2 extends WritableComparable<K2>> implements
		WritableComparable<NumberPairWritable<K1, K2>> {

	private K1 keyOne;
	private K2 keyTwo;

	/**
	 * @return the keyOne
	 */
	public K1 getKeyOne() {
		return keyOne;
	}

	/**
	 * @return the keyTwo
	 */
	public K2 getKeyTwo() {
		return keyTwo;
	}

	/**
	 * @param keyOne
	 *            the keyOne to set
	 */
	public void setKeyOne(K1 keyOne) {
		this.keyOne = keyOne;
	}

	/**
	 * @param keyTwo
	 *            the keyTwo to set
	 */
	public void setKeyTwo(K2 keyTwo) {
		this.keyTwo = keyTwo;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		keyOne.write(out);
		keyTwo.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		keyOne.readFields(in);
		keyTwo.readFields(in);
	}

	@Override
	public int compareTo(NumberPairWritable<K1, K2> o) {
		return keyOne.compareTo(o.keyOne);
	}

}
