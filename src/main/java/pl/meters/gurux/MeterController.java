package pl.meters.gurux;


import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import pl.meters.gurux.dto.MeterParams;

import java.util.List;

@RestController
@RequiredArgsConstructor
public class MeterController {

    private final MeterService meterService;

    @PostMapping("/read")
    public List<Object[]> getReadings(@RequestBody MeterParams meterParams) {
        return meterService.readMeter(meterParams);
    }

}
