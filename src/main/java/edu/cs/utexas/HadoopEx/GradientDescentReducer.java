package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GradientDescentReducer extends  Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    double sum_n;
    double sum_res;
    double sum_XRes;

    public void reduce(Text text, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        for (DoubleWritable value : values) {
            if (text.toString().equals("n")) { //if this is adding to the count of numbers
                sum_n += value.get();
            } else if (text.toString().equals("sumRes")) { //if we are summing the y - (mx + b)
                sum_res += value.get();
            } else if (text.toString().equals("sumXRes")) { //if we are summing x (y - (mx + b))
                sum_XRes += value.get();
            }
        }

   }
   
   public void cleanup(Context context) throws IOException, InterruptedException {

        double alpha = Double.parseDouble(context.getConfiguration().get("alpha"));
        double m = Double.parseDouble(context.getConfiguration().get("m"));
        double b = Double.parseDouble(context.getConfiguration().get("b"));

        double twoOverN = - 2 / sum_n;
        double mseM = twoOverN * sum_XRes; 
        double mseB = twoOverN * sum_res;

        m = m - (alpha * mseM); //calculate new m and b
        b = b - (alpha * mseB); 

        //print out parameters for this iteration
        context.write(new Text("Slope"), new DoubleWritable(m));
        context.write(new Text("Intercept"), new DoubleWritable(b));
   }
}