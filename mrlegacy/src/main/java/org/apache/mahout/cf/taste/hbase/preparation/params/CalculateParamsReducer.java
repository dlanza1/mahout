package org.apache.mahout.cf.taste.hbase.preparation.params;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class CalculateParamsReducer extends
		TableReducer<Text, Text, ImmutableBytesWritable> {
	
	private static int RM4_MIN_USERS;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {

		RM4_MIN_USERS = context.getConfiguration().getInt("prepareparams.rm4.min_users", 6);
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		String s_key = key.toString();
		
		if(s_key.startsWith("RM1-"))
			reduceRM1(key, values, context);
		else if(s_key.startsWith("RM2-"))
			reduceRM2(key, values, context);
		else if(s_key.startsWith("RM3-"))
			reduceRM3(key, values, context);
		else if(s_key.startsWith("RM4-"))
			reduceRM4(key, values, context);
		else
			throw new IOException("Tipo de parámetro no válido.");
		
		// TODO Escribir timestamps
		
	}

	/**
	 * RM1: Número de items que ha votado u
	 * @param key
	 * @param values
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void reduceRM1(
			Text key,
			Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		
		Iterator<Text> it = values.iterator();
		if(!it.hasNext())
			throw new IOException("RM1: invalid number of values.");
		
		String id_user = key.toString().replaceFirst("RM1-", "");
		String number_of_recommendations = it.next().toString();
		
		if(it.hasNext())
			throw new IOException("RM1: invalid number of values.");
		
		Put put = new Put(id_user.getBytes());
		put.add(Bytes.toBytes("param"), Bytes.toBytes("RM1"), 
				number_of_recommendations.getBytes());
		
		context.write(new ImmutableBytesWritable(Bytes.toBytes("r_users")), put);
	}

	/**
	 * RM2: Número de usuarios que han votado a i
	 * @param key
	 * @param values
	 * @param context
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	private void reduceRM2(
			Text key,
			Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		
		String id_item = key.toString().replaceFirst("RM2-", "");
		String number_of_recommendations = values.iterator().next().toString();
		
		Put put = new Put(id_item.getBytes());
		put.add(Bytes.toBytes("param"), Bytes.toBytes("RM2"), 
				number_of_recommendations.getBytes());
		
		context.write(new ImmutableBytesWritable(Bytes.toBytes("r_items")), put);
	}

	/**
	 * RM3: Número de usuarios que han votado los items, que u ha votado
	 * @param key
	 * @param values
	 * @param context
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	private void reduceRM3(
			Text key,
			Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		
		Integer sum_users = 0;
		for (Text num_users : values)
			sum_users += Integer.parseInt(num_users.toString());
		
		String id_user = key.toString().replaceFirst("RM3-", "");
		
		Put put = new Put(id_user.getBytes());
		put.add(Bytes.toBytes("param"), Bytes.toBytes("RM3"), 
				sum_users.toString().getBytes());
		
		context.write(new ImmutableBytesWritable(Bytes.toBytes("r_users")), put);
	}

	/**
	 * RM4: Número de usuarios que han votado mas de x items, que u ha votado
	 * @param key
	 * @param values
	 * @param context
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	private void reduceRM4(
			Text key,
			Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		
		String id_user = key.toString().replaceFirst("RM4-", "");
		
		//Contamos el número de veces que se repite cada usuario
		//si se alcanza el límite sumamos uno
		Integer sum_users = 0;
		HashMap<Text, Integer> map = new HashMap<Text, Integer>();
		for (Text user : values)
			if(map.containsKey(user)){
				int sum = map.get(user) + 1;
				map.put(user, sum);
				
				if(sum == RM4_MIN_USERS)
					sum_users++;
			}else
				map.put(user, 1);
		
		Put put = new Put(id_user.getBytes());
		put.add(Bytes.toBytes("param"), Bytes.toBytes("RM4"), 
				sum_users.toString().getBytes());
		
		context.write(new ImmutableBytesWritable(Bytes.toBytes("r_users")), put);
	}
}
