package org.apache.mahout.cf.taste.hbase.preparation.params;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.util.Bytes;

public class KeyBetweenComparable extends WritableByteArrayComparable {

	int first_key;
	int last_key;
	
	public KeyBetweenComparable(){
		first_key = Integer.MIN_VALUE;
		last_key = Integer.MAX_VALUE;
	}

	public KeyBetweenComparable(int start_key, int end_key) {
		super();

		this.first_key = start_key;
		this.last_key = end_key;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Bytes.writeByteArray(out, Bytes.toBytes(first_key));
		Bytes.writeByteArray(out, Bytes.toBytes(last_key));
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first_key = Bytes.toInt(Bytes.readByteArray(in));
		last_key = Bytes.toInt(Bytes.readByteArray(in));
	}

	@Override
	public int compareTo(byte[] key, int offset, int length) {
		int row_key = Bytes.toInt(key, offset, length);

		return row_key >= first_key && row_key <= last_key ? 0 : 1;
	}

}
