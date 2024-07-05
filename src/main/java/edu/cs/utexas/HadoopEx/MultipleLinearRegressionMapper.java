package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MultipleLinearRegressionMapper extends Mapper<Object, Text, Text, DoubleWritable> {

	double[] betas;

	public void setup(Context context) {
		betas = new double[5];
		for (int i = 0; i < betas.length; i++) { //get betas from context
			betas[i] = Double.parseDouble(context.getConfiguration().get("beta" + i));
		}
	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split(",");

		if (passesChecks(tokens)) {

			//get the x values from the input data
			double[] x = { Double.parseDouble(tokens[4]), Double.parseDouble(tokens[5]), Double.parseDouble(tokens[11]), 
				Double.parseDouble(tokens[15])};

			double y = Double.parseDouble(tokens[16]);

			//get our prediction
			double prediction = betas[0];
			for (int i = 0; i < betas.length - 1; i++) {
				prediction += betas[i + 1] * x[i];
			}

			//find the error and send it to the reducer
			double error = prediction - y;
			context.write(new Text("beta0"), new DoubleWritable(error)); //write beta values to context
			for (int i = 0; i < betas.length - 1; i++) {
				context.write(new Text("beta" + (i + 1)), new DoubleWritable(error * x[i]));
			}

		}
	
	}	

	//check if this line is valid to be used
	public boolean passesChecks(String[] tokens) {
		if (tokens.length != 17) {
			return false;
		}
	
		try {
			double fare_amount = Double.parseDouble(tokens[11]);
			double trip_distance = Double.parseDouble(tokens[5]);
			double tolls_amount = Double.parseDouble(tokens[15]);
			if (fare_amount < 3.00 || fare_amount > 200.00) { return false; } 
			if (trip_distance < 1.00 || trip_distance > 50.00) { return false; } 
			if (tolls_amount < 3.00) { return false; } 
		} catch (NumberFormatException e) {
			return false;
		}

		return true;
	}
}