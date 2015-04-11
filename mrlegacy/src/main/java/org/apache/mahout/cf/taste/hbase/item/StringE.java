package org.apache.mahout.cf.taste.hbase.item;

import java.nio.charset.Charset;

public class StringE {
	
	public static String toString(byte[] bytes) {
		int i; for (i = 0; i < bytes.length && bytes[i] != 0; i++);
		
		return new String(bytes, 0, i, Charset.defaultCharset());
	}
	
}
