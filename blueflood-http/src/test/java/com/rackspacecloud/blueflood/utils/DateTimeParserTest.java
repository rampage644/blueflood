package com.rackspacecloud.blueflood.utils;

import com.rackspacecloud.blueflood.utils.DateTimeParser;
import junit.framework.Assert;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;

public class DateTimeParserTest {
    @Test
    public void testFromUnixTimestamp() {
        long unixTimestamp = 1422792604;

        Assert.assertEquals(DateTimeParser.parse(Long.toString(unixTimestamp)),
                new DateTime(unixTimestamp));
    }

    @Test
    public void testPlainTimeDateFormat() {
        // %H:%M%Y%m%d
        DateTimeFormatter formatter = DateTimeFormat.forPattern("HH:mmyyyyMMdd");
        String dateTimeWithSpace = "10:55 2014 12 20";
        String dateTimeWithUnderscore = "10:55_2014_12_20";

        Assert.assertEquals(DateTimeParser.parse(dateTimeWithSpace),
                new DateTime(formatter.parseDateTime(dateTimeWithSpace.replace(" ", ""))));

        Assert.assertEquals(DateTimeParser.parse(dateTimeWithUnderscore),
                new DateTime(formatter.parseDateTime(dateTimeWithUnderscore.replace("_", ""))));
    }

    @Test
    public void testNowKeyword() {
        String nowTimestamp = "now";

        Assert.assertEquals(DateTimeParser.parse(nowTimestamp),
                new DateTime());
    }

    @Test
    public void testTimezone() {
        String timestampWithTimezone = "now + 06:00";
        String timestampWithZeroTimeZone = "now + 00:00";

        Assert.assertEquals(DateTimeParser.parse(timestampWithTimezone),
                DateTimeParser.parse(timestampWithZeroTimeZone).plusHours(6));
    }

    @Test
    public void testRegularHourMinute() {
        String hourMinuteTimestamp = "12:24";
        String hourMinuteWithAm = "9:13am";
        String hourMinuteWithPm = "09:13pm";

        Assert.assertEquals(DateTimeParser.parse(hourMinuteTimestamp),
                referenceDateTime().withHourOfDay(12).withMinuteOfHour(24));

        Assert.assertEquals(DateTimeParser.parse(hourMinuteWithAm),
                referenceDateTime().withHourOfDay(9).withMinuteOfHour(13));

        Assert.assertEquals(DateTimeParser.parse(hourMinuteWithPm),
                referenceDateTime().withHourOfDay(21).withMinuteOfHour(13));
    }

    @Test
    public void testHourMinuteKeywords() {
        String noonTimestamp = "noon";
        String teatimeTimestamp = "teatime";
        String midnightTimestamp = "midnight";

        Assert.assertEquals(DateTimeParser.parse(noonTimestamp),
                referenceDateTime().withHourOfDay(12).withMinuteOfHour(0));

        Assert.assertEquals(DateTimeParser.parse(teatimeTimestamp),
                referenceDateTime().withHourOfDay(16).withMinuteOfHour(0));

        Assert.assertEquals(DateTimeParser.parse(midnightTimestamp),
                referenceDateTime().withHourOfDay(0).withMinuteOfHour(0));
    }

    @Test
    public void testDayKeywords() {
        String todayTimestamp = "today";
        String yesterdayTimestamp = "yesterday";
        String tomorrowTimeStamp = "tomorrow";

        Assert.assertEquals(DateTimeParser.parse(todayTimestamp),
                referenceDateTime());

        Assert.assertEquals(DateTimeParser.parse(yesterdayTimestamp),
                referenceDateTime().minusDays(1));

        Assert.assertEquals(DateTimeParser.parse(tomorrowTimeStamp),
                referenceDateTime().plusDays(1));
    }

    @Test
    public void testDateFormats() {
        testFormat("12/30/14", new DateTime(2014, 12, 30, 0, 0, 0, 0));
        testFormat("12/30/2014", new DateTime(2014, 12, 30, 0, 0, 0, 0));
        testFormat("Jul 30", new DateTime(2014, 07, 30, 0, 0, 0, 0));
        testFormat("20141230", new DateTime(2014, 12, 30, 0, 0, 0, 0));
    }

    @Test
    public void testDayOfWeekFormat() {
        DateTime date = DateTimeParser.parse("Sun");
        Assert.assertEquals(date.getDayOfWeek(), 7);
    }

    private void testFormat(String dateString, DateTime date) {
        Assert.assertEquals(DateTimeParser.parse(dateString), date);
    }

    private static DateTime referenceDateTime() {
        return new DateTime().withSecondOfMinute(0).withMillisOfSecond(0);
    }

}
