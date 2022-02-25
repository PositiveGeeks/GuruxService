package pl.meters.gurux.dlms;

import gurux.dlms.GXDateTime;
import gurux.dlms.objects.GXDLMSObject;
import gurux.dlms.objects.GXDLMSProfileGeneric;

public interface DLMSReader {


    /**
     * Initializes connection.
     */
    void initializeConnection() throws Exception;

    void closeConnection() throws Exception;

    /**
     * Read association view.
     */
    void readAssociationView() throws Exception;


    /**
     * Reads selected DLMS object with selected attribute index.
     */
    Object read(GXDLMSObject item, int attributeIndex) throws Exception;


    /**
     * Read Profile Generic's data by range (start and end time).
     */
    Object[] readRowsByRange(final GXDLMSProfileGeneric pg,
                             final GXDateTime start, final GXDateTime end) throws Exception;

    public void getProfileGenericColumns();

    public void readScalerAndUnits() throws Exception;

    public void getReadOut();

}
