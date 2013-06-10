/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-5-5
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.util.compound;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * CompoundKeyComparator
 * 
 * @author Macthink
 * @param <V>
 * @param <K>
 */
public class CompoundKeyComparator<K extends WritableComparable<K>, V extends WritableComparable<V>> extends
		WritableComparator {

	protected CompoundKeyComparator() {
		super(CompoundKeyValuePair.class, true);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		CompoundKeyValuePair<K, V> o1 = (CompoundKeyValuePair<K, V>) a;
		CompoundKeyValuePair<K, V> o2 = (CompoundKeyValuePair<K, V>) b;
		return o1.getFirst().compareTo(o2.getFirst());
	}

}
