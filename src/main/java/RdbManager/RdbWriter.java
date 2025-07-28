package RdbManager;
import java.io.ByteArrayOutputStream;

import StorageManager.*;

public class RdbWriter {
    private StringStorage stringStorage;

    public RdbWriter(StringStorage stringStorage){
        this.stringStorage = stringStorage;
    }

    public byte[] serializeToRdb(){
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeHeader(out);
        writeDbSelector(out);
        writeHashTableSizes(out);
        writeStringKeys(out);
        writeEndOfFileMarker(out);
        return out.toByteArray();
    }

    void writeHeader(ByteArrayOutputStream out){
        // "REDIS0012" in ASCII: 52 45 44 49 53 30 30 31 32
        out.writeBytes(new byte[] {0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x32});
    }

    private void writeDbSelector(ByteArrayOutputStream out) {
        out.write(0xFE);
        out.write(0); // DB index 0
    }

    private void writeHashTableSizes(ByteArrayOutputStream out) {
        out.write(0xFB);
        // Key-value hash table size
        writeRdbSize(out, stringStorage.getAllKeys().size());
        // Expires hash table size
        int expiryCount = 0;
        for (String key : stringStorage.getAllKeys()) {
            if (stringStorage.getExpiryTime(key) != null) expiryCount++;
        }
        writeRdbSize(out, expiryCount);
    }

    private void writeStringKeys(ByteArrayOutputStream out) {
        for (String key : stringStorage.getAllKeys()) {
            Long expiry = stringStorage.getExpiryTime(key);
            if (expiry != null) {
                // Write expiry in ms
                out.write(0xFC);
                for (int j = 0; j < 8; j++) {
                    out.write((int) ((expiry >> (8 * j)) & 0xFF));
                }
            }
            // Write value type (0 = string)
            out.write(0);
            writeRdbString(out, key);
            String value = stringStorage.get(key);
            writeRdbString(out, value);
        }
    }

    private void writeEndOfFileMarker(ByteArrayOutputStream out) {
        out.write(0xFF);
    }

    private void writeRdbString(ByteArrayOutputStream out, String s) {
        writeRdbSize(out, s.length());
        out.writeBytes(s.getBytes());
    }

    private void writeRdbSize(ByteArrayOutputStream out, int size) {
        if (size < 0x40) {
            out.write(size);
        } else if (size < 0x4000) {
            out.write(((size >> 8) & 0x3F) | 0x40);
            out.write(size & 0xFF);
        } else {
            out.write(0x80);
            out.write((size >> 24) & 0xFF);
            out.write((size >> 16) & 0xFF);
            out.write((size >> 8) & 0xFF);
            out.write(size & 0xFF);
        }
    }
}