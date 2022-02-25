package pl.meters.gurux.dto;

import lombok.*;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MeterParams {
    private String meterProducer;
    private String hostName;
    private Short port;
    private Integer clientAddress;
    private Integer logicalAddress;
    private Integer physicalAddress;
    private String authenticationStr;
    private Boolean useLogicalNameReferencing;
    private LocalDateTime from;
    private LocalDateTime to;
    private int clockShortName;
    private String clockObis;
    private int profileToReadSn;
    private String profileToReadLn;
    private List<String> fieldsOfProfileLn = new ArrayList<>();
}
