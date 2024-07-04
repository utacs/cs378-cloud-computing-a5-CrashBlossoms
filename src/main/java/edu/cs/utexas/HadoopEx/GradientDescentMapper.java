package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GradientDescentMapper extends Mapper<Object, Text, Text, DoubleWritable> {

	// Create a counter and initialize with 1
	//private final IntWritable counter = new IntWritable(1);
	// Create a hadoop text object to store words
	//private Text word = new Text();

	private double m;
    private double b;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Read the current parameters (m, b) from the context
        m = Double.parseDouble(context.getConfiguration().get("m"));
        b = Double.parseDouble(context.getConfiguration().get("b"));
    }

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split(",");

		if (tokens.length == 17) { //if line has all necessary fields
			
			double x; //distance
			double y; //fare
			
			try {
				x = Float.parseFloat(tokens[5]);
				y = Float.parseFloat(tokens[11]);
			} catch (NumberFormatException e) {
				return;
			}

			context.write(new Text("n"), new DoubleWritable(1)); //count number of points we have

			double res = y - (m * x + b);
			
			context.write(new Text("sumRes"), new DoubleWritable(res));
			context.write(new Text("sumXRes"), new DoubleWritable(x * res));
		}
	}
}