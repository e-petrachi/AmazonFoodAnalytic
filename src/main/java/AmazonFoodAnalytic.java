import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class AmazonFoodAnalytic extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(AmazonFoodAnalytic.class);

    private AmazonFoodAnalytic() {}

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new AmazonFoodAnalytic(), args);
    }

    @Override
    @SuppressWarnings({ "static-access" })
    public int run(String[] args) throws Exception {

        long startTime = System.currentTimeMillis();

        Job job = new Job(new Configuration(), "AmazonFoodAnalytic");

        job.setJarByClass(AmazonFoodAnalytic.class);

        job.setMapperClass(AmazonFoodAnalyticMapperOne.class);
        //job.setCombinerClass(AmazonFoodAnalyticReducerOne.class);
        job.setReducerClass(AmazonFoodAnalyticReducerOne.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(false);

        LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        return 0;
    }
}
