package mrspark.job1;

import mapreduce.AmazonFoodConstants;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Int;
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
        conf.set("spark.hadoop.validateOutputSpecs", "false");

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
        }).filter(Objects::nonNull).filter( rc -> rc.getYEAR() >= 1999);


        JavaPairRDD<Tuple2<Integer,String>, Integer> years2word2occ = map4years_word(data)
                .reduceByKey((a, b) -> a + b );

        JavaPairRDD<Integer, Tuple2<Integer,String>> occ2years2word = years2word2occ
                .mapToPair( x -> new Tuple2<>(x._2(),x._1()) ).sortByKey(false);

        JavaPairRDD<Integer, Tuple2<Integer, String>> top = occ2years2word
                .mapToPair(y -> new Tuple2<>(y._2()._1(), new Tuple2<>(y._1(), y._2()._2())));

        JavaPairRDD<Integer, Iterable<Tuple2<Integer, String>>> result = top.groupByKey().sortByKey();

        JavaPairRDD<Integer, List<Tuple2<Integer, String>>> stampato = result
                .mapToPair( x -> {
                    ArrayList<Tuple2<Integer, String>> t = new ArrayList<>();
                    int i = 0;

                    for (Tuple2<Integer,String> el : x._2()){
                        if(i < 10)
                            t.add(el);
                        else
                            break;
                        i++;
                    }
                    return new Tuple2<>(x._1(),t);
                });

        // args[1] = path and namefile
        stampato.coalesce(1).saveAsTextFile(args[1]);

        LOG.info("\n\n\n\n\n\nJob Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds\n\n\n\n\n\n");


        sc.stop();

    }


    private static JavaPairRDD<Tuple2<Integer,String>, Integer> map4years_word(JavaRDD<ReviewsConstants> data) {
        return data.flatMapToPair( rc -> {

            StringTokenizer tokenizer = new StringTokenizer(rc.getSUMMARY(), "[\'_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"]");

            //StringTokenizer tokenizer = new StringTokenizer(rc.getSUMMARY(), " \t\n\r\f,.:;?![]'");
            ArrayList<Tuple2<Tuple2<Integer,String>, Integer>> list2tuples = new ArrayList<>();
            while (tokenizer.hasMoreTokens()){
                list2tuples.add(new Tuple2<>(new Tuple2<>(
                        rc.getYEAR(), tokenizer.nextToken().toLowerCase().replaceAll("[^a-zA-Z0-9]", "")),1));
            }
            return list2tuples.iterator();
        });
    }


}
