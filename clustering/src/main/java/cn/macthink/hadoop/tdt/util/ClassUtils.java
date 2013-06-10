/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-5-31
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.util;

import java.lang.reflect.InvocationTargetException;

/**
 * ClassUtils
 * 
 * @author Macthink
 */
public class ClassUtils {

	private ClassUtils() {
	}

	public static <T> T instantiateAs(String classname, Class<T> asSubclassOfClass) {
		try {
			return instantiateAs(Class.forName(classname).asSubclass(asSubclassOfClass), asSubclassOfClass);
		} catch (ClassNotFoundException e) {
			throw new IllegalStateException(e);
		}
	}

	public static <T> T instantiateAs(String classname, Class<T> asSubclassOfClass, Class<?>[] params, Object[] args) {
		try {
			return instantiateAs(Class.forName(classname).asSubclass(asSubclassOfClass), asSubclassOfClass, params,
					args);
		} catch (ClassNotFoundException e) {
			throw new IllegalStateException(e);
		}
	}

	public static <T> T instantiateAs(Class<? extends T> clazz, Class<T> asSubclassOfClass, Class<?>[] params,
			Object[] args) {
		try {
			return clazz.asSubclass(asSubclassOfClass).getConstructor(params).newInstance(args);
		} catch (InstantiationException ie) {
			throw new IllegalStateException(ie);
		} catch (IllegalAccessException iae) {
			throw new IllegalStateException(iae);
		} catch (NoSuchMethodException nsme) {
			throw new IllegalStateException(nsme);
		} catch (InvocationTargetException ite) {
			throw new IllegalStateException(ite);
		}
	}

	public static <T> T instantiateAs(Class<? extends T> clazz, Class<T> asSubclassOfClass) {
		try {
			return clazz.asSubclass(asSubclassOfClass).getConstructor().newInstance();
		} catch (InstantiationException ie) {
			throw new IllegalStateException(ie);
		} catch (IllegalAccessException iae) {
			throw new IllegalStateException(iae);
		} catch (NoSuchMethodException nsme) {
			throw new IllegalStateException(nsme);
		} catch (InvocationTargetException ite) {
			throw new IllegalStateException(ite);
		}
	}
}
