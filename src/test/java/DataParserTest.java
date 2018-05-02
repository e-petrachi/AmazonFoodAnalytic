import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.Calendar;
import java.util.Date;

import static org.junit.Assert.assertEquals;

public class DataParserTest {

    private long datalong = 1303862400;
    private String datastring = "1303862400";
    private Date date;
    private Calendar calendar1;
    private Calendar calendar2;
    private Calendar calendar3;

    @Before
    public void instances() {
        date = Date.from(Instant.ofEpochSecond(datalong));
        calendar1 = Calendar.getInstance();
        calendar1.setTime(date);

        date = Date.from(Instant.ofEpochSecond(Long.parseLong(datastring)));
        calendar2 = Calendar.getInstance();
        calendar2.setTime(date);

        date = Date.from(Instant.ofEpochMilli(System.currentTimeMillis()));
        calendar3 = Calendar.getInstance();
        calendar3.setTime(date);
    }
    @Test
    public void parseYearValue(){
        int year = calendar1.get(Calendar.YEAR);
        assertEquals(year,2011);
    }
    @Test
    public void parseYearToYear(){
        int year1 = calendar1.get(Calendar.YEAR);
        int year2 = calendar2.get(Calendar.YEAR);
        assertEquals(year1,year2);
    }
    @Test
    public void parseThisYear(){
        int year = calendar3.get(Calendar.YEAR);
        assertEquals(year,2018);
    }

}
