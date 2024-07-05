package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MultipleLinearRegressionReducer extends  Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private double[] betas;
    private double alpha;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        betas = new double[5];
        for (int i = 0; i < betas.length; i++) { //get betas and alpha from context
            betas[i] = Double.parseDouble(context.getConfiguration().get("beta" + i));
        }
        alpha = Double.parseDouble(context.getConfiguration().get("alpha"));
    }

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0;  //sum of errors
        double count = 0; //number of lines (data points) we have processed

        for (DoubleWritable value : values) {
            sum += value.get();
            count += 1;
        }

        if (!key.toString().equals("count")) {
            int index = Integer.parseInt(key.toString().substring(4));
            betas[index] = betas[index] - (alpha * (sum / count));
        }

    }

    //
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (int i = 0; i < betas.length; i++) {
            context.write(new Text("beta" + i), new DoubleWritable(betas[i]));
        }
    }
}