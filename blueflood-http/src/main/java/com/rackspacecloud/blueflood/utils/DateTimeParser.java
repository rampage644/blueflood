package com.rackspacecloud.blueflood.utils;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class DateTimeParser {
    public static DateTime parse(String s) {
        String stringToParse = s.replace(" ", "").replace(",", "").replace("_", "");
        if (StringUtils.isNumeric(stringToParse)) {
            return new DateTime(Long.parseLong(stringToParse));
        }
        try {
            DateTimeFormatter formatter = DateTimeFormat.forPattern("HH:mmyyyyMMdd");
            return formatter.parseDateTime(stringToParse);
        }
        catch (IllegalArgumentException e) {

        }

        String timezone = "";
        if (stringToParse.contains("+")) {
            String[] timezoneSplit = stringToParse.split("\\+", 1);
            stringToParse = timezoneSplit[0];
            timezone = timezoneSplit.length > 1 ? timezoneSplit[1] : "";
        } else if (stringToParse.contains("-")) {
            String[] timezoneSplit = stringToParse.split("-", 1);
            stringToParse = timezoneSplit[0];
            timezone = timezoneSplit.length > 1 ? timezoneSplit[1] : "";
        }

        return parseTime(stringToParse);
    }

    private static DateTime parseTime(String stringToParse) {
        DateTime resultDateTime = new DateTime().withTime(0, 0, 0, 0);

        if (stringToParse.equals("now"))
            return resultDateTime;

        if (stringToParse.contains(":")) {
            int hour = 0;
            int minute = 0;
            int semicolonPosition = stringToParse.indexOf(":");
            if (semicolonPosition != -1) {
                hour = Integer.parseInt(stringToParse.substring(0, semicolonPosition));
                minute = Integer.parseInt(stringToParse.substring(semicolonPosition + 1, semicolonPosition + 3));
                stringToParse = stringToParse.substring(semicolonPosition + 3);

                try {
                    if (stringToParse.substring(0, 2).equals("am")) {
                        stringToParse = stringToParse.substring(2);
                    } else if (stringToParse.substring(0, 2).equals("pm")) {
                        hour = (hour + 12) % 24;
                        stringToParse = stringToParse.substring(2);
                    }
                }
                catch (StringIndexOutOfBoundsException e) {}
            }
            resultDateTime = resultDateTime.withHourOfDay(hour).withMinuteOfHour(minute);
        }

        if (stringToParse.startsWith("noon")) {
            resultDateTime = resultDateTime.withHourOfDay(12).withMinuteOfHour(0);
            stringToParse = stringToParse.substring(0, "noon".length());
        } else if (stringToParse.startsWith("teatime")) {
            resultDateTime = resultDateTime.withHourOfDay(16).withMinuteOfHour(0);
            stringToParse = stringToParse.substring(0, "teatime".length());
        } else if (stringToParse.startsWith("midnight")) {
            resultDateTime = resultDateTime.withHourOfDay(0).withMinuteOfHour(0);
            stringToParse = stringToParse.substring(0, "midnight".length());
        }

        if (stringToParse.equals("today")) {
            // do nothing
        } else if (stringToParse.equals("tomorrow")) {
            resultDateTime = resultDateTime.plusDays(1);
        } else if (stringToParse.equals("yesterday")) {
            resultDateTime = resultDateTime.minusDays(1);
        } else {
            // do nothing
        }

        String[] datePatterns = {"MM/dd/YY", "MM/dd/YYYY", "YYYYMMdd", "MMMMdd", "EEE"};
        for (String s : datePatterns) {
            try {
                DateTime date = parseDate(stringToParse, s);
                resultDateTime = resultDateTime.withDate(date.year().get(), date.monthOfYear().get(), date.dayOfMonth().get());
                break;
            }
            catch (IllegalArgumentException e) {
                continue;
            }
        }


        return resultDateTime;
    }

    private static DateTime parseDate(String stringToParse, String format) {
        return DateTimeFormat.forPattern(format).parseDateTime(stringToParse);
    }
}
