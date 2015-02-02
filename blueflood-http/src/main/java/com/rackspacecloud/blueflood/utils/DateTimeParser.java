package com.rackspacecloud.blueflood.utils;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DateTimeParser {
    public static DateTime parse(String s) {
        String stringToParse = s.replace(" ", "").replace(",", "").replace("_", "");
        if (StringUtils.isNumeric(stringToParse)) {
            if (stringToParse.length() == 8 &&
                    Integer.parseInt(stringToParse.substring(0, 4)) > 1900 &&
                    Integer.parseInt(stringToParse.substring(4, 6)) < 13 &&
                    Integer.parseInt(stringToParse.substring(6)) < 32) {
                // do nothing
            } else {
                return new DateTime(Long.parseLong(stringToParse) * 1000);
            }
        }
        try {
            DateTimeFormatter formatter = DateTimeFormat.forPattern("HH:mmyyyyMMdd");
            return formatter.parseDateTime(stringToParse);
        }
        catch (IllegalArgumentException e) {

        }

        String offset;
        if (stringToParse.contains("+")) {
            String[] offsetSplit = stringToParse.split("\\+", 2);
            stringToParse = offsetSplit[0];
            offset = offsetSplit.length > 1 ? offsetSplit[1] : "";
            DateTime dateTime = parseTime(stringToParse);
            dateTime = parseOffset(dateTime, offset);
            return  dateTime;
        } else if (stringToParse.contains("-")) {
            String[] offsetSplit = stringToParse.split("-", 2);
            stringToParse = offsetSplit[0];
            offset = offsetSplit.length > 1 ? "-" + offsetSplit[1] : "";
            DateTime dateTime = parseTime(stringToParse);
            dateTime = parseOffset(dateTime, offset);
            return dateTime;
        } else {
            return parseTime(stringToParse);
        }
    }

    private static DateTime parseOffset(DateTime baseDateTime, String offset) {
        if (offset.equals(""))
            return baseDateTime;
        Pattern p = Pattern.compile("(-?\\d*)([a-z]*)");
        Matcher m = p.matcher(offset);
        if (!m.matches())
            return baseDateTime;

        int count = Integer.parseInt(m.group(1));
        String unit = m.group(2);

        DateTime dateTimeWithOffset;
        if (unit.startsWith("s")) {
            dateTimeWithOffset = baseDateTime.plusSeconds(count);
        } else if (unit.startsWith("min")) {
            dateTimeWithOffset = baseDateTime.plusMinutes(count);
        } else if (unit.startsWith("h")) {
            dateTimeWithOffset = baseDateTime.plusHours(count);
        } else if (unit.startsWith("d")) {
            dateTimeWithOffset = baseDateTime.plusDays(count);
        } else if (unit.startsWith("mon")) {
            dateTimeWithOffset = baseDateTime.plusMonths(count);
        } else if (unit.startsWith("y")) {
            dateTimeWithOffset = baseDateTime.plusYears(count);
        } else {
            dateTimeWithOffset = baseDateTime;
        }

        return dateTimeWithOffset;
    }

    private static DateTime parseTime(String stringToParse) {
        DateTime resultDateTime = new DateTime().withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0);

        if (stringToParse.equals("") || stringToParse.equals("now"))
            return new DateTime().withSecondOfMinute(0).withMillisOfSecond(0);

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
            stringToParse = stringToParse.substring("noon".length());
        } else if (stringToParse.startsWith("teatime")) {
            resultDateTime = resultDateTime.withHourOfDay(16).withMinuteOfHour(0);
            stringToParse = stringToParse.substring("teatime".length());
        } else if (stringToParse.startsWith("midnight")) {
            resultDateTime = resultDateTime.withHourOfDay(0).withMinuteOfHour(0);
            stringToParse = stringToParse.substring("midnight".length());
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

        String[] datePatterns = {"MM/dd/YY", "MM/dd/YYYY", "YYYYMMdd", "MMMMddYYYY"};
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

        try {
            String monthDayOptionalYearFormat = "MMMMdd";
            DateTime date = parseDate(stringToParse, monthDayOptionalYearFormat);
            resultDateTime = resultDateTime.withDate(resultDateTime.getYear(), date.getMonthOfYear(), date.getDayOfMonth());
        }
        catch (IllegalArgumentException e) {

        }

        try {
            String dayOfWeekFormat = "EEE";
            DateTime date = parseDate(stringToParse, dayOfWeekFormat);
            while (resultDateTime.getDayOfWeek() != date.getDayOfWeek())
                resultDateTime = resultDateTime.minusDays(1);
        }
        catch (IllegalArgumentException e) {

        }





        return resultDateTime;
    }

    private static DateTime parseDate(String stringToParse, String format) {
        return DateTimeFormat.forPattern(format).parseDateTime(stringToParse);
    }
}
