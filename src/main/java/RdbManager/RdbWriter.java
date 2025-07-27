package RdbManager;
import java.io.ByteArrayOutputStream;

import StorageManager.*;

public class RdbWriter {
    private StringStorage stringStorage;
    private ListStorage listStorage;
    private StreamStorage streamStorage;

    public RdbWriter(StringStorage stringStorage, ListStorage listStorage, StreamStorage streamStorage){
        this.stringStorage = stringStorage;
        this.listStorage = listStorage;
        this.streamStorage = streamStorage;
    }

    byte[] serializeToRdb(){
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        // Add header to result
        writeHeader(out);
        // Other functions
        return out.toByteArray();
    }

    void writeHeader(ByteArrayOutputStream out){
        // "REDIS0012" in ASCII: 52 45 44 49 53 30 30 31 32
        out.writeBytes(new byte[] {52, 45, 44, 49, 53, 30, 30, 31, 32});
    }
}
