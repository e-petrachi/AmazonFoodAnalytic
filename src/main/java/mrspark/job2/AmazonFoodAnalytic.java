package mrspark.job2;

import mapreduce.AmazonFoodConstants;
import mrspark.ReviewsConstants;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Int;
import scala.Tuple2;

import java.io.PrintWriter;
import java.util.*;

public class AmazonFoodAnalytic {

    private static final Logger LOG = Logger.getLogger(AmazonFoodAnalytic.class);
    static { LOG.setLevel(Level.DEBUG);}

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        SparkConf conf = new SparkConf().setAppName("mrspark.job2.AmazonFoodAnalytic");
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

            int score;
            try {
                score = Integer.parseInt(fields[AmazonFoodConstants.SCORE]);
            } catch (Exception e){
                return null;
            }

            return new ReviewsConstants(fields[AmazonFoodConstants.PRODUCTID],time,score);
        }).filter(Objects::nonNull).filter(x -> x.getYEAR() >= AmazonFoodConstants.YEARFROM && x.getYEAR() <= AmazonFoodConstants.YEARTO);

        JavaPairRDD<Tuple2<String,Integer>, Tuple2<Double, Integer>> id2year2score2count = data
                .mapToPair( rc -> new Tuple2<>(new Tuple2<>(rc.getPRODUCTID(),rc.getYEAR()), new Tuple2<>((double) rc.getSCORE(),1)))
                .reduceByKey((a,b) -> new Tuple2<>(a._1() + b._1(),a._2() + b._2()));

        JavaPairRDD<Tuple2<String,Integer>, Double> id2year2avgscore = id2year2score2count
                .mapToPair( x -> new Tuple2<>(x._1(), Math.round(x._2()._1()/x._2()._2() * 100.0)/100.0 ));

        JavaPairRDD<String, Tuple2<Integer,Double>> result = id2year2avgscore
                .mapToPair( x -> new Tuple2<>(x._1()._1(), new Tuple2<>(x._1()._2(),x._2())));

        JavaPairRDD<String, Iterable<Tuple2<Integer,Double>>> stampato = result.groupByKey().sortByKey();

        // args[1] = path and namefile
        stampato.coalesce(1).saveAsTextFile(args[1]);


        LOG.info("\n\n\n\n\n\nJob Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds\n\n\n\n\n\n");

        sc.stop();

    }


}
