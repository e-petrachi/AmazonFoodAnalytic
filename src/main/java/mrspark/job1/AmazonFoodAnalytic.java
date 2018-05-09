package mrspark.job1;

import mapreduce.AmazonFoodConstants;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Int;
import scala.Tuple2;
import mrspark.ReviewsConstants;

import java.util.*;

public class AmazonFoodAnalytic {

    private static final Logger LOG = Logger.getLogger(AmazonFoodAnalytic.class);
    static { LOG.setLevel(Level.DEBUG);}

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("mrspark.job1.AmazonFoodAnalytic");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // args[0] = path and namefile.csv
        JavaRDD<String> csvdata = sc.textFile(args[0]);

        JavaRDD<ReviewsConstants> data = csvdata.map((String csvline) -> {
            String[] fields = csvline.split(",");

            long time;
            try {
                time = Long.parseLong(fields[AmazonFoodConstants.TIME]);
            } catch (Exception e){
                return null;
            }
            return new ReviewsConstants(time,fields[AmazonFoodConstants.SUMMARY]);
        });

        // uncomment for debug
        //data.foreach(rc -> { if (rc != null) { LOG.debug(rc.toString()); } });

        //JavaPairRDD<Integer, String> years2word = map4years(data);

        // uncomment for debug
        //years2word.foreach(y2w -> { LOG.debug(y2w._1.toString() + " : " + y2w._2.toString()); });

        JavaPairRDD<Tuple2<Integer,String>, Integer> years2word = map4years_word(data);
        JavaPairRDD<Tuple2<Integer,String>, Integer> years2word2occ = reduce4words(years2word);


        List<Tuple2<Integer, Tuple2<Integer,String>>> top = top10words(years2word2occ);

        // uncomment for debug
        //reduce4words.foreach(y2w -> { LOG.debug(y2w._1._1.toString() + "," + y2w._1._2.toString() + " : " + y2w._2.toString()); });

        for (Tuple2<Integer, Tuple2<Integer,String>> tuple : top){
            LOG.debug(tuple._1() + ":" + tuple._2()._1() + "," + tuple._2()._2());
        }

        LOG.debug("FINE!");

    }

    private static JavaPairRDD<Integer, String> map4years(JavaRDD<ReviewsConstants> data) {
        JavaPairRDD<Integer, String> couples = data.filter(Objects::nonNull).flatMapToPair((ReviewsConstants rc) -> {

            StringTokenizer tokenizer = new StringTokenizer(rc.getSUMMARY(), " \t\n\r\f,.:;?![]'");
            ArrayList<Tuple2<Integer, String>> list2tuples = new ArrayList<>();
            while (tokenizer.hasMoreTokens()) {
                list2tuples.add(new Tuple2<>(rc.getYEAR(), tokenizer.nextToken().toLowerCase().replaceAll("[^a-zA-Z0-9]", "")));
            }
            return list2tuples.iterator();

        });
        return couples;
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

    private static List<Tuple2<Integer, Tuple2<Integer,String>>> top10words_mod(JavaPairRDD<Tuple2<Integer,String>, Integer> years2word2count) {

        JavaPairRDD<Integer, Tuple2<Integer,String>> year2rest = years2word2count.mapToPair( y -> new Tuple2<>(y._1()._1(), new Tuple2<>(y._2(),y._1()._2())) );

        HashMap<Integer, List<Tuple2<Integer,String>>> hash = new HashMap<>();

        year2rest.foreach( year -> {
            List<Tuple2<Integer,String>> l;
            if (hash.containsKey(year._1())) {
                l = new ArrayList<>();
            } else {
                l = hash.get(year._1());
            }
            l.add(new Tuple2<>(year._2()._1(), year._2()._2()));
            hash.put(year._1(), l);

        });


        for (Integer year : hash.keySet()){
            List<Tuple2<Integer,String>> lists = hash.get(year);
            // TODO continua da qui!!!

        }



        List<Tuple2<Integer, Tuple2<Integer,String>>> top10 =
                years2word2count.mapToPair( y2w2c -> new Tuple2<>(y2w2c._2(), new Tuple2<>(y2w2c._1()._1(),y2w2c._1()._2())))
                .sortByKey(false)
                .take(10);

        return top10;

    }
    private static List<Tuple2<Integer, Tuple2<Integer,String>>> top10words(JavaPairRDD<Tuple2<Integer,String>, Integer> years2word2count) {

        List<Tuple2<Integer, Tuple2<Integer,String>>> top10 =
                years2word2count.mapToPair( y2w2c -> new Tuple2<>(y2w2c._2(), new Tuple2<>(y2w2c._1()._1(),y2w2c._1()._2())))
                        .sortByKey(false)
                        .take(10);

        return top10;

    }


}
