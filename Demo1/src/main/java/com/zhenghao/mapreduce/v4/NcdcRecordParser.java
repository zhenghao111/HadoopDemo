package com.zhenghao.mapreduce.v4;

public class NcdcRecordParser {

    private static final int MISSING_TEMPERATURE = 9999;

    private String year;
    private int airTemperature;
    private boolean airTemperatureMalformed;
    private String quality;

    public void parser(String record) {
        year = record.substring(15, 19);

    }
}
