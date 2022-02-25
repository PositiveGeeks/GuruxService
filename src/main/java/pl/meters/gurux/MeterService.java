package pl.meters.gurux;


import gurux.common.enums.TraceLevel;
import gurux.dlms.GXDLMSClient;
import gurux.dlms.GXDateTime;
import gurux.dlms.enums.Authentication;
import gurux.dlms.enums.InterfaceType;
import gurux.dlms.objects.GXDLMSCaptureObject;
import gurux.dlms.objects.GXDLMSClock;
import gurux.dlms.objects.GXDLMSObject;
import gurux.dlms.objects.GXDLMSProfileGeneric;
import gurux.dlms.secure.GXDLMSSecureClient;
import gurux.net.GXNet;
import gurux.net.enums.NetworkType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RestController;
import pl.meters.gurux.converters.ItronReadingConverter;
import pl.meters.gurux.converters.MeterReadingConverter;
import pl.meters.gurux.converters.ReadingsConverter;
import pl.meters.gurux.dlms.DLMSReader;
import pl.meters.gurux.dlms.DLMSReaderImpl;
import pl.meters.gurux.dto.MeterParams;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RestController
@Slf4j
public class MeterService {

    public List<Object[]> readMeter(MeterParams meterParams) {
        List<Object[]> resList = new ArrayList<>();
        DLMSReader reader = null;
        LocalDateTime readingStart = LocalDateTime.now();
        String hostname = meterParams.getHostName();
        Short port = meterParams.getPort();

        log.info("start reading profile on meter {} : {}, timeFrom = {}, timeTo {}"
                , hostname, port, meterParams.getFrom(), meterParams.getTo());
        try {
            reader = createReader(meterParams);
            openConnection(reader, meterParams);
            resList = readMeter(reader, meterParams);

        } catch (Exception e) {
            log.info("problem with reading  meter {} : {} ", hostname, port);
            log.info(e.getMessage());
        } finally {
            logReadingTime(readingStart, hostname, port);
            if (reader != null) {
                try {
                    closeConnection(reader, meterParams);
                } catch (Exception e) {
                    log.info("problem with closing connection meter {} : {} , meter may be blocked :(", hostname, port);
                    log.error(e.getMessage(), e);
                }
            }
        }
        return resList;
    }

    private List<Object[]> readMeter(DLMSReader reader, MeterParams meterParams) throws Exception {

        GXDateTime from = localDateTime2GXDateTime(meterParams.getFrom());
        GXDateTime to = localDateTime2GXDateTime(meterParams.getTo());

        int snProfileGeneric = meterParams.getProfileToReadSn();
        String obisProfileGeneric = meterParams.getProfileToReadLn();
        GXDLMSProfileGeneric p = new GXDLMSProfileGeneric(obisProfileGeneric, snProfileGeneric);
        List<Map.Entry<GXDLMSObject, GXDLMSCaptureObject>> obj = readCaptureObject(p, reader, meterParams);
        traceHeader(obj);
        Object[] cells = reader.readRowsByRange(p, from, to);
        traceCells(cells);

        ReadingsConverter converter = (meterParams.getMeterProducer().equalsIgnoreCase("ITRON")) ? new ItronReadingConverter() : new MeterReadingConverter();
        return converter.convert(cells, obj, meterParams);
    }

    GXDateTime localDateTime2GXDateTime(LocalDateTime date) {
        return new GXDateTime(date.getYear(), date.getMonthValue(), date.getDayOfMonth(), date.getHour(), date.getMinute(), 0, 0);
    }

    private List<Map.Entry<GXDLMSObject, GXDLMSCaptureObject>> readCaptureObject(GXDLMSProfileGeneric p, DLMSReader reader, MeterParams meterParams) throws Exception {
        String hostname = meterParams.getHostName();
        Short port = meterParams.getPort();
        p.setSortObject(new GXDLMSClock(meterParams.getClockObis(), meterParams.getClockShortName()));
        //Before you can read the buffer you need to read the capture object. Read attribute index 3 and after that, you can read buffer. https://www.gurux.fi/node/13954
        log.info("reading captureObject on meter, {} : {}", hostname, port);
        return (List<Map.Entry<GXDLMSObject, GXDLMSCaptureObject>>) reader.read(p, 3);
    }


    private DLMSReader createReader(MeterParams meterParams) {
        GXDLMSSecureClient client = new GXDLMSSecureClient(meterParams.getUseLogicalNameReferencing());
        Authentication auth = Authentication.valueOf(meterParams.getAuthenticationStr());
        client.setAuthentication(auth);
        client.setClientAddress(meterParams.getClientAddress());
        client.setInterfaceType(InterfaceType.HDLC);
        client.setServerAddress(GXDLMSClient.getServerAddress(meterParams.getLogicalAddress(), meterParams.getPhysicalAddress()));
        GXNet media = new GXNet();
        media.setHostName(meterParams.getHostName());
        media.setPort(meterParams.getPort());
        media.setTrace(TraceLevel.INFO);
        media.setProtocol(NetworkType.TCP);
        return new DLMSReaderImpl(client, media, TraceLevel.INFO, false);
    }


    private void openConnection(DLMSReader reader, MeterParams meterParams) throws Exception {
        LocalDateTime initConnectionStart = LocalDateTime.now();
        log.info("start connection to meter on {} : {} ", meterParams.getHostName(), meterParams.getPort());
        reader.initializeConnection();
        LocalDateTime initConnectionStop = LocalDateTime.now();
        Duration durationInit = Duration.between(initConnectionStart, initConnectionStop);
        log.info("open connection time for meter  {} : {}  in sec: {}", meterParams.getHostName(), meterParams.getPort(), durationInit.getSeconds());
    }

    private void closeConnection(DLMSReader reader, MeterParams meterParams) throws Exception {
        log.info("close connection on meter on {} : {} ", meterParams.getHostName(), meterParams.getPort());
        reader.closeConnection();
        log.info("connection successfully closed on meter {} : {} ", meterParams.getHostName(), meterParams.getPort());
    }

    private static void logReadingTime(LocalDateTime profileGenericStart, String hostname, int port) {
        LocalDateTime profileGenericStop = LocalDateTime.now();
        Duration profileGenericReading = Duration.between(profileGenericStart, profileGenericStop);
        log.info("time reading profile for meter {} : {}  in sec: {}", hostname, port, profileGenericReading.getSeconds());
    }

    private static void traceHeader(List<Map.Entry<GXDLMSObject, GXDLMSCaptureObject>> obj) {
        int i = 1;
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<GXDLMSObject, GXDLMSCaptureObject> row : obj) {
            sb.append(String.format("%-25s", i + ": " + row.getKey().getLogicalName()));
            i++;
        }
        log.info("Headers of profile: {}", sb.toString());
    }


    static void traceCells(Object[] cells) {
        StringBuilder sb = new StringBuilder();
        int rowNb = 1;
        for (Object rows : cells) {
            sb.setLength(0);
            int i = 1;
            for (Object cell : (Object[]) rows) {
                String pattern = (i < 3) ? "%-25s" : "%-15s";
                sb.append(String.format(pattern, i + ": " + cell));
                i++;
            }
            log.info("row number: {} = {}", rowNb++, sb.toString());
        }
        log.info("===========================================");
    }

}
