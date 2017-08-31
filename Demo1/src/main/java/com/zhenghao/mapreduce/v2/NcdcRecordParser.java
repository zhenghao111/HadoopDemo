package com.zhenghao.mapreduce.v2;

import org.apache.hadoop.io.Text;

public class NcdcRecordParser {

    public static final int MISSING_TEMPERATURE = 9999;

    private String year;
    private int airTemperature;
    private String quality;

    public void parser(String record) {
        year = record.substring(15, 19);
        String airTempeatureString;
        if (record.charAt(87) == '+') {
            airTempeatureString = record.substring(88, 92);
        } else {
            airTempeatureString = record.substring(87, 92);
        }

        airTemperature = Integer.parseInt(airTempeatureString);
        quality = record.substring(92, 93);
    }

    public void parser(Text record) {
        parser(record.toString());
    }

    public boolean isValidTemperature() {
        return  airTemperature != MISSING_TEMPERATURE && quality.matches("[01459]");
    }

    public String getYear() {
        return year;
    }

    public int getAirTemperature() {
        return airTemperature;
    }

}
