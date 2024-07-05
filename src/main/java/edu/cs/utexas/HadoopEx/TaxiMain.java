package edu.cs.utexas.HadoopEx;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TaxiMain extends Configured implements Tool {
	
	/**
	 * 
	 * @param args
	 * @throws Exception
	 */

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Driver needs two arguments: input path and output path!");
			System.exit(-1);
		}

		int res = ToolRunner.run(new Configuration(), new TaxiMain(), args);
		System.exit(res);
	}

	public int run(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		doTask1(args[0], "SLR_" + args[1]);
		System.out.println("FINISHED JOB 1");

		doTask2(args[0], "GRD_" + args[1]);
		System.out.println("FINISHED JOB 2");

		doTask3(args[0], "MGD_" + args[1]);
		System.out.println("FINISHED JOB 3");

		return 0;
	}

	//Simple Linear Regression task
	public static int doTask1(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Simple Linear Regression");
		job1.setJarByClass(TaxiMain.class);
        job1.setMapperClass(LinearRegressionMapper.class);
        job1.setReducerClass(LinearRegressionReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job1, new Path(inputPath));
		job1.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job1, new Path(outputPath));
		job1.setOutputFormatClass(TextOutputFormat.class);

		return (job1.waitForCompletion(true) ? 0 : 1);
	}
	
	//Gradient Descent task
	public static void doTask2(String inputPath, String outputPathPart) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf2 = new Configuration();

		double alpha = 0.001; // Learning rate
        int iterations = 100; // Number of iterations
        double m = 0.0; // Initial slope
        double b = 0.0; // Initial intercept
		conf2.set("alpha", String.valueOf(alpha));

		for (int i = 0; i < iterations; i++) {

			conf2.set("m", String.valueOf(m));
			conf2.set("b", String.valueOf(b));
			String outputPath = outputPathPart + "_iter_" + i;

			Job job2 = Job.getInstance(conf2, "Gradient Descent");
			job2.setJarByClass(TaxiMain.class);
			job2.setMapperClass(GradientDescentMapper.class);
			job2.setReducerClass(GradientDescentReducer.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(DoubleWritable.class);

			FileInputFormat.addInputPath(job2, new Path(inputPath));
			job2.setInputFormatClass(TextInputFormat.class);
	
			FileOutputFormat.setOutputPath(job2, new Path(outputPath));
			job2.setOutputFormatClass(TextOutputFormat.class);

			job2.waitForCompletion(true);

			// update m and b 
            Path outputFilePath = new Path(outputPath, "part-r-00000");
            FileSystem fs = FileSystem.get(conf2);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(outputFilePath)));
			double new_m = Double.parseDouble((br.readLine().split("	"))[1]); //get the next value of m from output file
			double new_b = Double.parseDouble((br.readLine().split("	"))[1]); //get the next value of b from output file
			br.close();
			m = new_m;
			b = new_b;
		}
	}

	//Multiple Linear Regression task
	public static void doTask3(String inputPath, String outputPathPart) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf3 = new Configuration();

		int iterations = 100;
		double alpha = 0.001; // Learning rate
        double[] betas = new double[5]; // Initial parameters

		for (int i = 0; i < iterations; i++) {

			conf3.set("alpha", String.valueOf(alpha));
			//set the betas in the context so we can access them in the job
			for (int j = 0; j < betas.length; j++) {
				conf3.set("beta" + j, String.valueOf(betas[j])); 
			}

			String outputPath = outputPathPart + "_iter_" + i;
			Job job3 = Job.getInstance(conf3, "Multiple Linear Regression");
			job3.setJarByClass(TaxiMain.class);
			job3.setMapperClass(MultipleLinearRegressionMapper.class);
			job3.setReducerClass(MultipleLinearRegressionReducer.class);
			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(DoubleWritable.class);

			FileInputFormat.addInputPath(job3, new Path(inputPath));
			job3.setInputFormatClass(TextInputFormat.class);
	
			FileOutputFormat.setOutputPath(job3, new Path(outputPath));
			job3.setOutputFormatClass(TextOutputFormat.class);

			job3.waitForCompletion(true);

			// update the betas for next iteration 
            Path outputFilePath = new Path(outputPath, "part-r-00000");
            FileSystem fs = FileSystem.get(conf3);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(outputFilePath)));
			String line = br.readLine();
			while (line != null) {
				String[] tokens = line.split("	");
				int index = Integer.parseInt(tokens[0].substring(4));
				betas[index] = Double.parseDouble(tokens[1]); //get beta value from output file
				line = br.readLine();
			}
			br.close();

			//clean up folders with past results behind us
			File f = new File(outputPathPart + "_iter_" + (i -1));
			if (f.exists()) {
				deleteDirectory(f);
			}
			f.delete();
		}
	}

	//deletes a directory if it exists, structure from geeksforgeeks.org
	public static void deleteDirectory(File file) {
		if (file == null) {
			return;
		}
	
        for (File subfile : file.listFiles()) {

            if (subfile.isDirectory()) {
                deleteDirectory(subfile);
            }
        
			subfile.delete();
        }
    }

}