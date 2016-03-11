package com.lucid.ycsb;

import com.lucid.common.LogUtils;
import com.lucid.spanner.ReadQuery;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Status;
import io.atomix.copycat.Query;

import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

public abstract class YCSBDB extends DB {

    private static final String LOG_TAG = "YCSB_DB";

    private YCSBClient ycsbClient;

    public YCSBDB() {
        this.ycsbClient = getYCSBClient();
        Batcher.getInstance().setYCSBClient(ycsbClient);
    }

    // The returned object must be singleton
    public abstract YCSBClient getYCSBClient();

    @Override
    public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        try {
            String qualifiedKey = YCSBUtils.createQualifiedKey(table, key);
            Query<String> query = new ReadQuery(qualifiedKey);
            String value = ycsbClient.executeQuery(query);
            if (value != null) {
                YCSBUtils.fromJson(value, fields, result);
            }
            return Status.OK;
        } catch (Exception e) {
            LogUtils.error(LOG_TAG, "Failed to read", e);
            return Status.ERROR;
        }
    }

    @Override
    public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        return Status.NOT_IMPLEMENTED;
    }

    @Override
    public Status update(String table, String key, HashMap<String, ByteIterator> values) {
        return Status.NOT_IMPLEMENTED;
    }

    @Override
    public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
        try {
            String qualifiedKey = YCSBUtils.createQualifiedKey(table, key);
            WriteObject writeObject = new WriteObject();
            writeObject.setKey(qualifiedKey);
            writeObject.setValue(YCSBUtils.toJson(values));
            if (Batcher.getInstance().write(writeObject)) {
                return Status.OK;
            }
            return Status.ERROR;
        } catch (Exception e) {
            LogUtils.error(LOG_TAG, "Failed to write", e);
            return Status.ERROR;
        }
    }

    @Override
    public Status delete(String table, String key) {
        return Status.NOT_IMPLEMENTED;
    }
}
