package mrspark.job3;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

public class AscendingSerializableComparator implements Comparator, Serializable {

    @Override
    public int compare(Object o1, Object o2) {
        Tuple2<String,String> t1 = (Tuple2<String,String>) o1;
        Tuple2<String,String> t2 = (Tuple2<String,String>) o2;

        return t1._1().concat(t1._2()).compareTo(t2._1().concat(t2._2()));
    }
}
