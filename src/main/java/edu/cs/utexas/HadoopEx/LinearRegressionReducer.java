package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LinearRegressionReducer extends  Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    double sumX = 0;
    double sumY = 0; 
    double sumXY = 0; 
    double sumX2 = 0; 
    double count = 0;

    public void reduce(Text text, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        
        for (DoubleWritable value : values) {
            if (text.toString().equals("sumX")) {
                sumX += value.get();
            } else if (text.toString().equals("sumY")) {
                sumY += value.get();
            } else if (text.toString().equals("sumXY")) {
                sumXY += value.get();
            } else if (text.toString().equals("sumX2")) {
                sumX2 += value.get();
            } else if (text.toString().equals("count")) {
                count += value.get();
            }
        }
   }
   
   public void cleanup(Context context) throws IOException, InterruptedException {
        double denom_result = ((count * sumX2) - (sumX * sumX));
        double slope = ((count * sumXY) - (sumX * sumY)) / denom_result;
        double intercept = ((sumX2 * sumY) - (sumX - sumXY)) / denom_result;

        context.write(new Text("Slope"), new DoubleWritable(slope));
        context.write(new Text("Intercept"), new DoubleWritable(intercept));
   }
}