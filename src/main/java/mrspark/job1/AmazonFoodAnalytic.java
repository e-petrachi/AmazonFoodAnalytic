package mrspark.job1;

import com.google.common.collect.Lists;
import mapreduce.AmazonFoodConstants;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Function1;
import scala.Int;
import scala.Tuple2;
import mrspark.ReviewsConstants;

import java.io.PrintWriter;
import java.util.*;

public class AmazonFoodAnalytic {

    private static final Logger LOG = Logger.getLogger(AmazonFoodAnalytic.class);
    static { LOG.setLevel(Level.DEBUG);}

    public static void main(String[] args) throws Exception {
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


        JavaPairRDD<Tuple2<Integer,String>, Integer> years2word = map4years_word(data);
        JavaPairRDD<Tuple2<Integer,String>, Integer> years2word2occ = reduce4words(years2word);
        JavaPairRDD<Integer, Tuple2<Integer, String>> top = top10words_mod(years2word2occ);

        List<Integer> keys = top.keys().distinct().collect();

        PrintWriter writer = new PrintWriter(args[1], "UTF-8");
        for (Integer k : keys){
            List<Tuple2<Integer,String>> list_rdd = getRDDValues(top,k);
            JavaPairRDD<Integer,String> rdd = sc.parallelizePairs(list_rdd);
            List<Tuple2<Integer,String>> rdd2 = rdd.sortByKey(false).take(10);
            //JavaPairRDD<Integer,String> rdd3 = sc.parallelizePairs(rdd2);

            writer.print("" + k + "\t");
            rdd2.forEach( row -> writer.print(row._2().toString() + ":" + row._1().toString() + " | " ));
            writer.print("\n");
            //LOG.debug("ANNO:" + k);
            //rdd2.forEach(d -> LOG.debug(d));


        }
        writer.close();


        LOG.debug("FINE!");

        sc.stop();

    }

    private static List<Tuple2<Integer,String>> getRDDValues(JavaPairRDD<Integer, Tuple2<Integer, String>> ss, Integer key){
        return ss.lookup(key);
    }

    private static JavaPairRDD<Tuple2<Integer,String>, Integer> map4years_word(JavaRDD<ReviewsConstants> data) {
        JavaPairRDD<Tuple2<Integer,String>, Integer> couples = data.filter(Objects::nonNull).flatMapToPair( rc -> {

            StringTokenizer tokenizer = new StringTokenizer(rc.getSUMMARY(), " \t\n\r\f,.:;?![]'");
            ArrayList<Tuple2<Tuple2<Integer,String>, Integer>> list2tuples = new ArrayList<>();
            while (tokenizer.hasMoreTokens()){
                list2tuples.add(new Tuple2<>(new Tuple2<>(
                        rc.getYEAR(), tokenizer.nextToken().toLowerCase().replaceAll("[^a-zA-Z0-9]", "")),1));
            }
            return list2tuples.iterator();
        });

        return couples;
    }

    private static JavaPairRDD<Tuple2<Integer,String>, Integer> reduce4words(JavaPairRDD<Tuple2<Integer,String>, Integer> years2word2occ) {
        return years2word2occ.reduceByKey((a, b) -> a + b );
    }

    private static JavaPairRDD<Integer, Tuple2<Integer,String>> top10words_mod(JavaPairRDD<Tuple2<Integer,String>, Integer> years2word2count) {
        return years2word2count.mapToPair(y -> new Tuple2<>(y._1()._1(), new Tuple2<>(y._2(), y._1()._2())));
    }
    private static JavaPairRDD<Integer, Iterable<Tuple2<Integer,String>>> groupBYkey(JavaPairRDD<Integer, Tuple2<Integer, String>> year2rest){
        return year2rest.groupByKey().sortByKey();
    }
    private static JavaPairRDD<Tuple2<Integer,Integer>, String> nonso(JavaPairRDD<Tuple2<Integer, String>, Integer> years2word2occ){
        return years2word2occ.mapToPair( y2w2c -> new Tuple2<>(new Tuple2<>(y2w2c._1()._1(),y2w2c._2()),y2w2c._1()._2()));
    }


}
