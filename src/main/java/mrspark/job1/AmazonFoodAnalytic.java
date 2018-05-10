package mrspark.job1;

import mapreduce.AmazonFoodConstants;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import mrspark.ReviewsConstants;

import java.io.PrintWriter;
import java.util.*;

public class AmazonFoodAnalytic {

    private static final Logger LOG = Logger.getLogger(AmazonFoodAnalytic.class);
    static { LOG.setLevel(Level.DEBUG);}

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        SparkConf conf = new SparkConf().setAppName("mrspark.job1.AmazonFoodAnalytic");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // args[0] = path and namefile.csv
        JavaRDD<String> csvdata = sc.textFile(args[0]);

        JavaRDD<ReviewsConstants> data = csvdata.map( csvline -> {
            String[] fields = csvline.split(",");

            long time;
            try {
                time = Long.parseLong(fields[AmazonFoodConstants.TIME]);
            } catch (Exception e){
                return null;
            }
            return new ReviewsConstants(time,fields[AmazonFoodConstants.SUMMARY]);
        });


        JavaPairRDD<Tuple2<Integer,String>, Integer> years2word2occ = map4years_word(data)
                .reduceByKey((a, b) -> a + b );

        JavaPairRDD<Integer, Tuple2<Integer, String>> top = years2word2occ
                .mapToPair(y -> new Tuple2<>(y._1()._1(), new Tuple2<>(y._2(), y._1()._2())))
                .filter( y -> y._1().intValue() >= 1999);

        ArrayList<Integer> keys = new ArrayList<>(top.keys().distinct().collect());
        Collections.sort(keys, (x,y) -> Integer.compare(x,y));

        // args[1] = path and namefile.txt
        PrintWriter writer = new PrintWriter(args[1], "UTF-8");

        for (Integer k : keys){
            List<Tuple2<Integer,String>> rdd = sc.parallelizePairs(top.lookup(k))
                    .sortByKey(false)
                    .take(10);

            writer.print("" + k + "\t");
            rdd.forEach( row -> writer.print(row._2().toString() + ":" + row._1().toString() + " | " ));

            writer.print("\n");
        }
        writer.close();

        LOG.info("\n\n\n\n\n\nJob Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds\n\n\n\n\n\n");

        sc.stop();

    }


    private static JavaPairRDD<Tuple2<Integer,String>, Integer> map4years_word(JavaRDD<ReviewsConstants> data) {
        return data.filter(Objects::nonNull).flatMapToPair( rc -> {

            StringTokenizer tokenizer = new StringTokenizer(rc.getSUMMARY(), " \t\n\r\f,.:;?![]'");
            ArrayList<Tuple2<Tuple2<Integer,String>, Integer>> list2tuples = new ArrayList<>();
            while (tokenizer.hasMoreTokens()){
                list2tuples.add(new Tuple2<>(new Tuple2<>(
                        rc.getYEAR(), tokenizer.nextToken().toLowerCase().replaceAll("[^a-zA-Z0-9]", "")),1));
            }
            return list2tuples.iterator();
        });
    }


}
