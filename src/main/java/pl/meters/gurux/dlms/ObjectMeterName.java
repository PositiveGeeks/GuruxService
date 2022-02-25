package pl.meters.gurux.dlms;

import lombok.*;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ObjectMeterName {
    String ln;
    int sn;
    String name;
}
