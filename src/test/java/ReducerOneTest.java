import com.opencsv.CSVParser;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;

import static org.junit.Assert.*;

public class ReducerOneTest {
    private Iterable<Text> values;
    private HashMap<String, Integer> word2count;
    private TreeMap<Integer, ArrayList<String>> count2values;
    private ArrayList<String> topTen;
    private Text result;

    private AmazonFoodAnalyticReducerOne reducer = new AmazonFoodAnalyticReducerOne();

    @Before
    public void instances() throws IOException {
        ArrayList<Text> list = new ArrayList<Text>();
        for (int i = 0; i < 10; i++) {
            list.add(new Text("enrico"));
        }
        for (int i = 0; i < 8; i++) {
            list.add(new Text("petrachi"));
        }
        for (int i = 0; i < 4; i++) {
            list.add(new Text("473004"));
            list.add(new Text("chiavarella"));
        }
        for (int i = 0; i < 12; i++) {
            list.add(new Text("carlo"));
        }
        for (int i = 0; i < 3; i++) {
            list.add(new Text("petracca"));
        }
        for (int i = 0; i < 2; i++) {
            list.add(new Text("altro"));
            list.add(new Text("bo"));
            list.add(new Text("nulla"));
            list.add(new Text("altrochè"));
        }
        for (int i = 0; i < 1; i++) {
            list.add(new Text("boh"));
            list.add(new Text("nullapiù"));
        }

        values = (Iterable<Text>) list;
        word2count = reducer.createHashWords(values);
        count2values = reducer.createInvertedIndex(word2count);
        topTen = reducer.createTopTenWords(count2values);
        result = reducer.getResults(topTen,word2count);

    }
    @Test
    public void createHashWords(){
        assertEquals(word2count.get("enrico"),new Integer(10));
    }
    @Test
    public void createInvertedIndex(){
        assertEquals(count2values.get(new Integer(10)).get(0),"enrico");
    }
    @Test
    public void createTopTenWords(){
        assertEquals(topTen.get(5),"petracca");
    }
    @Test
    public void getResults(){
        assertTrue(result.toString().contains("carlo") && result.toString().contains("petracca"));
    }

}
