package mapreduce.job1;

public class Temo {

    /*
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
        Job job = new Job(new Configuration(), "AmazonJob3");

        job.setJarByClass(AnalisysJob3.class);
        job.setMapperClass(MapperPart1Job3.class);
        job.setReducerClass(ReducerPart1Job3.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);


        Job job2 = new Job(new Configuration(), "AmazonJob3.2");
        job2.setJarByClass(AnalisysJob3.class);
        job2.setMapperClass(MapperPart2Job3.class);
        job2.setReducerClass(ReducerPart2Job3.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final"));

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        job2.waitForCompletion(true);

    }
    */
}
