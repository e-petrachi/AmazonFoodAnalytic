import com.opencsv.CSVParser;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.Calendar;
import java.util.Date;

import static org.junit.Assert.assertEquals;

public class CSVParserTest {

    private String csv = "1,B001E4KFG0,A3SGXH7AUHU8GW,delmartian,1,1,5,1303862400," +
            "Good Quality Dog Food,I have bought several of the Vitality canned dog food products and have found them all to be of good quality. " +
            "The product looks more like a stew than a processed meat and it smells better. My Labrador is finicky and she appreciates this product better than  most." +
            "2,B00813GRG4,A1D87F6ZCVE5NK,dll pa,0,0,1,1346976000," +
            "Not as Advertised,'Product arrived labeled as Jumbo Salted Peanuts...the peanuts were actually small sized unsalted. " +
            "Not sure if this was an error or if the vendor intended to represent the product as Jumbo.'";
    private String[] campi;

    private static final int TIME = 7;
    private static final int SUMMARY = 8;

    @Before
    public void instances() throws IOException {
        campi = new CSVParser().parseLine(csv);

    }
    @Test
    public void parseTime(){
        assertEquals(campi[TIME],"1303862400");
    }
    @Test
    public void parseSummary(){
        assertEquals(campi[SUMMARY].substring(0,1),"G");
    }


}
