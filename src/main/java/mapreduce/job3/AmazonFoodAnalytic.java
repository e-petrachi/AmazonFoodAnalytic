package mapreduce.job3;

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

        Job job = new Job(new Configuration(), "mapreduce.job3.AmazonFoodAnalytic");

        job.setJarByClass(AmazonFoodAnalytic.class);

        job.setMapperClass(AmazonFoodAnalyticMapper.class);
        job.setReducerClass(AmazonFoodAnalyticReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);

        Job job2 = new Job(new Configuration(), "mapreduce.job3.AmazonFoodAnalytic");

        job2.setJarByClass(AmazonFoodAnalytic.class);

        job2.setMapperClass(AmazonFoodAnalyticMapperTwo.class);
        job2.setReducerClass(AmazonFoodAnalyticReducerTwo.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final"));

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        job2.waitForCompletion(true);

        LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        return 0;
    }
}
