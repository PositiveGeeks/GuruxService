package pl.meters.gurux.dlms;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;


@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class ProfileReadParam {
    String ln;
    Integer sn;
    List<ObjectMeterName> fields = new ArrayList<>();
}
