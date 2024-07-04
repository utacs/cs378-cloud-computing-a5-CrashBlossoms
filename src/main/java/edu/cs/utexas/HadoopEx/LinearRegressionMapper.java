package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LinearRegressionMapper extends Mapper<Object, Text, Text, DoubleWritable> {

	// Create a counter and initialize with 1
	//private final IntWritable counter = new IntWritable(1);
	// Create a hadoop text object to store words
	// private Text word = new Text();
	int maps = 0;

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split(",");

		double distance; //X
		double fare; //Y

		if (tokens.length == 17) { //if line has all necessary fields
			try {
				distance = Float.parseFloat(tokens[5]);
				fare = Float.parseFloat(tokens[11]);
			} catch (NumberFormatException e) {
				return;
			}

			//suggested by ChatGPT
			context.write(new Text("sumX"), new DoubleWritable(distance));
			context.write(new Text("sumY"), new DoubleWritable(fare));
			context.write(new Text("sumXY"), new DoubleWritable(distance * fare));
			context.write(new Text("sumX2"), new DoubleWritable(distance * distance));
			context.write(new Text("count"), new DoubleWritable(1));
		}

		//need index 5 distance float
		// and index 11 fare amount float
	}
}