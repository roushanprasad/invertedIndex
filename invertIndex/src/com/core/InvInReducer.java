package com.core;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvInReducer extends Reducer<Text, Text, Text, Text>{

	public void reduce(final Text key, final Iterable<Text> values,
			final Context context ) throws IOException, InterruptedException{
		
		StringBuilder sb = new StringBuilder();
		
		for (Text text : values) {
			sb.append(text.toString());
			
			if(values.iterator().hasNext()){
				sb.append(" | ");
			}
		}
		context.write(key, new Text(sb.toString()));
	}
}
