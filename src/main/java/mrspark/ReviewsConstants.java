package mrspark;

import java.io.Serializable;
import java.time.Instant;
import java.util.Calendar;
import java.util.Date;

public class ReviewsConstants implements Serializable {
    private int ID;
    private String PRODUCTID;
    private String USERID;
    private String PROFILENAME;
    private int HelpfulnessNumerator;
    private int HelpfulnessDenominator;
    private int SCORE;
    private long TIME;
    private String SUMMARY;
    private String TEXT;
    private int YEAR;

    public ReviewsConstants(long TIME, String SUMMARY) {
        this.TIME = TIME;
        this.SUMMARY = SUMMARY;
        this.setYEAR();
    }
    public ReviewsConstants(String PRODUCTID, String USERID) {
        this.PRODUCTID = PRODUCTID;
        this.USERID = USERID;
    }
    public ReviewsConstants(String PRODUCTID,long TIME, int SCORE) {
        this.PRODUCTID = PRODUCTID;
        this.TIME = TIME;
        this.setYEAR();
        this.SCORE = SCORE;
    }

    public ReviewsConstants(int ID, String PRODUCTID, String USERID, int SCORE, long TIME, String SUMMARY) {
        this.ID = ID;
        this.PRODUCTID = PRODUCTID;
        this.USERID = USERID;
        this.SCORE = SCORE;
        this.TIME = TIME;
        this.SUMMARY = SUMMARY;
        this.setYEAR();
    }

    public ReviewsConstants(int ID, String PRODUCTID, String USERID, String PROFILENAME, int helpfulnessNumerator, int helpfulnessDenominator, int SCORE, long TIME, String SUMMARY, String TEXT) {
        this.ID = ID;
        this.PRODUCTID = PRODUCTID;
        this.USERID = USERID;
        this.PROFILENAME = PROFILENAME;
        HelpfulnessNumerator = helpfulnessNumerator;
        HelpfulnessDenominator = helpfulnessDenominator;
        this.SCORE = SCORE;
        this.TIME = TIME;
        this.SUMMARY = SUMMARY;
        this.TEXT = TEXT;
        this.setYEAR();
    }

    public int getID() {
        return ID;
    }

    public void setID(int ID) {
        this.ID = ID;
    }

    public String getPRODUCTID() {
        return PRODUCTID;
    }

    public void setPRODUCTID(String PRODUCTID) {
        this.PRODUCTID = PRODUCTID;
    }

    public String getUSERID() {
        return USERID;
    }

    public void setUSERID(String USERID) {
        this.USERID = USERID;
    }

    public String getPROFILENAME() {
        return PROFILENAME;
    }

    public void setPROFILENAME(String PROFILENAME) {
        this.PROFILENAME = PROFILENAME;
    }

    public int getHelpfulnessNumerator() {
        return HelpfulnessNumerator;
    }

    public void setHelpfulnessNumerator(int helpfulnessNumerator) {
        HelpfulnessNumerator = helpfulnessNumerator;
    }

    public int getHelpfulnessDenominator() {
        return HelpfulnessDenominator;
    }

    public void setHelpfulnessDenominator(int helpfulnessDenominator) {
        HelpfulnessDenominator = helpfulnessDenominator;
    }

    public int getSCORE() {
        return SCORE;
    }

    public void setSCORE(int SCORE) {
        this.SCORE = SCORE;
    }

    public long getTIME() {
        return TIME;
    }

    public void setTIME(long TIME) {
        this.TIME = TIME;
    }

    public String getSUMMARY() {
        return SUMMARY;
    }

    public void setSUMMARY(String SUMMARY) {
        this.SUMMARY = SUMMARY;
    }

    public String getTEXT() {
        return TEXT;
    }

    public void setTEXT(String TEXT) {
        this.TEXT = TEXT;
    }

    public int getYEAR() {
        return YEAR;
    }

    public void setYEAR() {
        Date date = Date.from(Instant.ofEpochSecond(this.TIME));
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        this.YEAR = calendar.get(Calendar.YEAR);
    }

    @Override
    public String toString() {
        return "ReviewsConstants{" +
                "YEAR=" + YEAR +
                '}';
    }
}
