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
 * CompoundKeyValueComparator
 * 
 * @author Macthink
 */
public class CompoundKeyValuePairComparator<K extends WritableComparable<K>, V extends WritableComparable<V>> extends
		WritableComparator {

	public CompoundKeyValuePairComparator() {
		super(CompoundKeyValuePair.class, true);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		CompoundKeyValuePair<K, V> o1 = (CompoundKeyValuePair<K, V>) a;
		CompoundKeyValuePair<K, V> o2 = (CompoundKeyValuePair<K, V>) b;
		if (!o1.getFirst().equals(o2.getFirst())) {
			return o1.getFirst().compareTo(o2.getFirst());
		} else {
			return o2.getSecond().compareTo(o1.getSecond());
		}
	}

}
