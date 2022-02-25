package pl.meters.gurux.converters;

import gurux.common.GXCommon;
import gurux.dlms.GXDLMSClient;
import gurux.dlms.GXDateTime;
import gurux.dlms.GXStructure;
import gurux.dlms.enums.DataType;
import gurux.dlms.objects.GXDLMSCaptureObject;
import gurux.dlms.objects.GXDLMSObject;
import lombok.extern.slf4j.Slf4j;
import pl.meters.gurux.dto.MeterParams;

import java.text.DateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class MeterReadingConverter implements ReadingsConverter {

    public List<Object[]> convert(Object[] cells, List<Map.Entry<GXDLMSObject, GXDLMSCaptureObject>> obj, MeterParams meterParams){
        List<Object[]> res = new ArrayList<>();

        int colNb_Clock = getColumnNumber(obj, meterParams.getClockObis());
        for (Object rows : cells) {
            Object[] resultRow = new Object[meterParams.getFieldsOfProfileLn().size() + 1];
            Object[] rowsTab = (Object[]) rows;
            Object clockObjFromMeter = rowsTab[colNb_Clock];
            resultRow[0] = getLocalDateTimeValue(clockObjFromMeter);
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


    int getColumnNumber(List<Map.Entry<GXDLMSObject, GXDLMSCaptureObject>> headers, String obis) {
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

    private  String getLocalDateTimeValue(Object obj) {
        String time = null;
        if (obj instanceof GXDateTime) {
            GXDateTime gxDateTime = (GXDateTime) obj;
            LocalDateTime dateTime = LocalDateTime.ofInstant(gxDateTime.getLocalCalendar().toInstant(), ZoneId.systemDefault());
            time = dateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        }
        return time;
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
}
