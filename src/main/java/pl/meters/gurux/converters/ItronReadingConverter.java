package pl.meters.gurux.converters;

import gurux.common.GXCommon;
import gurux.dlms.GXDLMSClient;
import gurux.dlms.GXDateTime;
import gurux.dlms.GXStructure;
import gurux.dlms.enums.DataType;
import gurux.dlms.enums.DateTimeSkips;
import gurux.dlms.objects.GXDLMSCaptureObject;
import gurux.dlms.objects.GXDLMSObject;
import lombok.extern.slf4j.Slf4j;
import pl.meters.gurux.dto.MeterParams;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Slf4j
public class ItronReadingConverter implements ReadingsConverter {


    public List<Object[]> convert(Object[] cells, List<Map.Entry<GXDLMSObject, GXDLMSCaptureObject>> obj, MeterParams meterParams) {
        List<Object[]> res = new ArrayList<>();
        LocalDateTime time = null;
        for (Object rows : cells) {
            Object[] resultRow = new Object[meterParams.getFieldsOfProfileLn().size() + 1];
            Object[] rowsTab = (Object[]) rows;
            boolean isReadingWithError = checkIfReadingWithError(rowsTab);
            time = getTime(rowsTab, isReadingWithError, time);
            if (isReadingWithError) {
                log.info("There is an error: {}" + time);
            }
            resultRow[0] = time.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            int i = 1;
            for (String obisOfField : meterParams.getFieldsOfProfileLn()) {
                int colNb = getColumnNumber(obj, obisOfField);
                Object readingsObjFromMeter = rowsTab[colNb];
                Double valueFromMeter = Double.valueOf(getValueAsString(readingsObjFromMeter));
                resultRow[i] = valueFromMeter;
                i++;
            }
            res.add(resultRow);

        }
        return res;
    }

    boolean checkIfReadingWithError(Object[] rowsTab) {
        Object timeInFirstColumn = readObjectAtPos(rowsTab, 0);
        Object timeIn2ndColumn = readObjectAtPos(rowsTab, 1);
        return timeWithErrorCode(timeInFirstColumn) || timeWithErrorCode(timeIn2ndColumn);
    }

    boolean timeWithErrorCode(Object obj) {
        if (obj == null) return false;
        GXStructure objStruct = (GXStructure) obj;
        Object _2ndElemReturnMeterValue = objStruct.get(1);
        boolean isOk = "000000".endsWith(_2ndElemReturnMeterValue.toString());
        return !isOk;
    }

    Object readObjectAtPos(Object[] rowsTab, int colNb) {
        return rowsTab[colNb];
    }


    String getValueAsString(Object obj) {
        String result = "";
        if (obj instanceof byte[]) {
            result = GXCommon.bytesToHex((byte[]) obj);
        } else if (obj instanceof GXStructure) {
            GXStructure objStruct = (GXStructure) obj;
            Object date = GXDLMSClient.changeType((byte[]) objStruct.get(0), DataType.DATETIME);
            result = date.toString();
        } else {
            result = String.valueOf(obj);
        }
        return result;
    }

    LocalDateTime getTime(Object[] rowsTab, boolean isReadingWithError, LocalDateTime prevReadingsTime) {
        LocalDateTime result = null;
        Object timeInFirstColumn = readObjectAtPos(rowsTab, 0);
        Object timeIn2ndColumn = readObjectAtPos(rowsTab, 1);
        if (isReadingWithError) {
            GXDateTime gXDateTime = geTimeAsGXDateTime((timeInFirstColumn != null) ? timeInFirstColumn : timeIn2ndColumn);
            result = getDataFromGXDateTime(gXDateTime, prevReadingsTime);
        } else {
            if (timeInFirstColumn != null) {
                GXDateTime gXDateTime = geTimeAsGXDateTime(timeInFirstColumn);
                result = getDataFromGXDateTime(gXDateTime, prevReadingsTime);
            } else {
                LocalDateTime prevReadingTimeTruncToSec = prevReadingsTime.withSecond(0).withNano(0);
                int near15 = getNear15Minute(prevReadingTimeTruncToSec.getMinute());
                if (near15 == 60) {
                    prevReadingTimeTruncToSec = prevReadingTimeTruncToSec.withMinute(0).plusHours(1);
                } else {
                    prevReadingTimeTruncToSec = prevReadingTimeTruncToSec.withMinute(near15);
                }
                result = prevReadingTimeTruncToSec.plusMinutes(15);
            }
        }
        return result;
    }

    LocalDateTime getDataFromGXDateTime(GXDateTime gxDateTime, LocalDateTime prevDate) {
        LocalDate localDate = null;
        LocalTime localTime = null;
        Calendar cal = gxDateTime.getLocalCalendar(TimeZone.getDefault());
        if (!gxDateTime.getSkip().contains(DateTimeSkips.YEAR) &&
                !gxDateTime.getSkip().contains(DateTimeSkips.MONTH)
                && !gxDateTime.getSkip().contains(DateTimeSkips.DAY)) {
            int year = cal.get(Calendar.YEAR);
            int month = cal.get(Calendar.MONTH) + 1;
            int day = cal.get(Calendar.DAY_OF_MONTH);
            localDate = LocalDate.of(year, month, day);
        }
        if (!gxDateTime.getSkip().contains(DateTimeSkips.MINUTE)) {
            int hour = cal.get(Calendar.HOUR_OF_DAY);
            int mm = cal.get(Calendar.MINUTE);
            int ss = cal.get(Calendar.SECOND);
            localTime = LocalTime.of(hour, mm, ss);
        }
        localDate = (null != localDate) ? localDate : prevDate.toLocalDate();
        localTime = (null != localTime) ? localTime : LocalTime.of(0, 0, 0);
        return LocalDateTime.of(localDate, localTime);
    }

    GXDateTime geTimeAsGXDateTime(Object obj) {
        GXDateTime gxDateTime = new GXDateTime();
        if (obj instanceof GXStructure) {
            GXStructure objStruct = (GXStructure) obj;
            Object haha = GXDLMSClient.changeType((byte[]) objStruct.get(0), DataType.DATETIME);
            gxDateTime = (GXDateTime) haha;
        }
        return gxDateTime;
    }

    public int getNear15Minute(int minutes) {
        if (minutes <= 0) {
            return 0;
        }
        if (minutes <= 15) {
            return 15;
        }
        if (minutes <= 30) {
            return 30;
        }
        if (minutes <= 45) {
            return 45;
        }
        return 60;
    }

    static int getColumnNumber(List<Map.Entry<GXDLMSObject, GXDLMSCaptureObject>> headers, String obis) {
        int columnNb = 0;
        boolean found = false;
        for (Map.Entry<GXDLMSObject, GXDLMSCaptureObject> col : headers) {
            if (obis.equals(col.getKey().getLogicalName())) {
                found = true;
                break;
            }
            columnNb++;
        }
        return found ? columnNb : 0;
    }
}
