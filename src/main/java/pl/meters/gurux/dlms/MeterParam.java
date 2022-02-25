package pl.meters.gurux.dlms;

import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

@Getter
@Setter
public class MeterParam {


    String ppe;

    Double ratio;
    LocalDateTime dateLastReadings;
    Double valueLastReading;
}
