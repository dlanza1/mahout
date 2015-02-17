package org.apache.mahout.cf.taste.hbase.preparation.params;

import java.io.IOException;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CalculateParamsMapper extends TableMapper<Text, Text> {

	private String table_name;
	
	@Override
	protected void setup(
			Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		table_name = Bytes.toString(((TableSplit) context.getInputSplit()).getTableName());
	}
	
	@Override
	protected void map(
			ImmutableBytesWritable key,
			Result result,
			Context context)
			throws IOException, InterruptedException {
		
		if(table_name.equals("r_users"))
			users_map(key, result, context);
		else if(table_name.equals("r_items"))
			items_map(key, result, context);		
	}

	private void users_map(ImmutableBytesWritable key, Result result,
			Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context) throws IOException, InterruptedException {

		int id_user = Integer.parseInt(Bytes.toString(key.get()));
		
//		RM1: Número de items que ha votado u
		NavigableMap<byte[], byte[]> colum_map = result.getFamilyMap(Bytes.toBytes("pref"));
		int number_of_preferences = colum_map.keySet().size();
		
		context.write(new Text("RM1-" + id_user), new Text("" + number_of_preferences));
	}
	
	private void items_map(ImmutableBytesWritable key, Result result,
			Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context) throws IOException, InterruptedException {
		
		int id_item = Integer.parseInt(Bytes.toString(key.get()));
		NavigableMap<byte[], byte[]> colum_map = result.getFamilyMap(Bytes.toBytes("pref"));
		
//		RM2: Número de usuarios que han votado a i
		int number_of_preferences = colum_map.keySet().size();
		context.write(new Text("RM2-" + id_item), new Text("" + number_of_preferences));
		
//		RM3: Número de usuarios que han votado los items, que u ha votado
		for(byte[] colum: colum_map.keySet()){
			String id_user = Bytes.toString(colum);
			
			context.write(new Text("RM3-" + id_user), new Text("" + (number_of_preferences - 1)));
		}
		
//		RM4: Número de usuarios que han votado mas de x items, que u ha votado
		for(byte[] u1: colum_map.keySet())
			for(byte[] u2: colum_map.keySet())
				if(u1 != u2){
					String id_u1 = Bytes.toString(u1);
					String id_u2 = Bytes.toString(u2);
					
					context.write(new Text("RM4-" + id_u1), new Text(id_u2));
				}
		
	}
}
