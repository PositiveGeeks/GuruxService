package pl.meters.gurux.converters;

import gurux.dlms.objects.GXDLMSCaptureObject;
import gurux.dlms.objects.GXDLMSObject;
import pl.meters.gurux.dto.MeterParams;

import java.util.List;
import java.util.Map;

public interface ReadingsConverter {

    List<Object[]> convert(Object[] cells, List<Map.Entry<GXDLMSObject, GXDLMSCaptureObject>> obj, MeterParams meterParams);
}
