package org.apache.mahout.cf.taste.hbase.preparation.params;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Calcula los siguientes parámetros
 * 
 * RM1: Número de items que ha votado u RM2: Número de usuarios que han votado a
 * i RM3: Número de usuarios que han votado los items, que u ha votado RM4:
 * Número de usuarios que han votado mas de 5 items, que u ha votado
 * 
 * @author daniellanzagarcia
 *
 */
public class PrepareParams extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new PrepareParams(), args);
		System.exit(res);

	}

	@Override
	public int run(String[] args) throws Exception {
		
		Job job = Job.getInstance(HBaseConfiguration.create());
		job.getConfiguration().set("hbase.zookeeper.quorum", "nodo1"); 
		job.getConfiguration().set("hbase.zookeeper.property.clientPort", "2181");
	    
		job.setJarByClass(PrepareParams.class);
		
		List<Scan> scans = new ArrayList<Scan>();

		Scan scan_users = new Scan();
		scan_users.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME,
				Bytes.toBytes("r_users"));
		scan_users.setCaching(500);
		scan_users.setCacheBlocks(false);
		scans.add(scan_users);

		Scan scan_items = new Scan();
		scan_items.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME,
				Bytes.toBytes("r_items"));
		scan_items.setCaching(500);
		scan_items.setCacheBlocks(false);
		scans.add(scan_items);
		
//		Scan scan_items2 = new Scan();
//		scan_items2.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME,
//				Bytes.toBytes("r_items"));
//		scan_items2.setCaching(500);
//		scan_items2.setFilter(new RowFilter(CompareOp.EQUAL, new KeyBetweenComparable(100, 200)));
//		scan_items2.setCacheBlocks(false);
//		scans.add(scan_items2);

		TableMapReduceUtil.initTableMapperJob(scans,
				CalculateParamsMapper.class, Text.class, Text.class, job);
		TableMapReduceUtil.initTableReducerJob("", CalculateParamsReducer.class, job);
		
		job.setOutputFormatClass(MultiTableOutputFormat.class);
		
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}

		return 0;
	}

}
