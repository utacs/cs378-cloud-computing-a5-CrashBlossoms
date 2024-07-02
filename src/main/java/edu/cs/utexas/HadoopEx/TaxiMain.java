package edu.cs.utexas.HadoopEx;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
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
		doJob1(args[0], "SLR_" + args[1]);
		System.out.println("FINISHED JOB 1");

		//doJob2(args[0], "GPS_" + args[1]);
		//System.out.println("FINISHED JOB 2");
		return 0;
	}

	public static int doJob1(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
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
	//java -jar target/Gradient-Descent-0.1-SNAPSHOT-jar-with-dependencies.jar SOME-Text-Fiel.txt  output

	public static int doJob2(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "GPS Erros");
		job2.setJarByClass(TaxiMain.class);
        job2.setMapperClass(GradientDescentMapper.class);
        job2.setReducerClass(GradientDescentReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job2, new Path(inputPath));
		job2.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job2, new Path(outputPath));
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		return (job2.waitForCompletion(true) ? 0 : 1);
	}
}
