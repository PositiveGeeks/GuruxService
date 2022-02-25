package pl.meters.gurux.dlms;

import gurux.dlms.enums.Authentication;
import gurux.dlms.enums.InterfaceType;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DlmsParam {
    int clientAddress;
    int logicalAddress;
    int physicalAddress;

    boolean useLogicalNameReferencing;
    InterfaceType interfaceType;
    Authentication authentication;
    String password;
}
