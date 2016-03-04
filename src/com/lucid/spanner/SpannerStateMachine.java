package com.lucid.spanner;

import com.lucid.common.LogUtils;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.Snapshottable;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.storage.snapshot.SnapshotReader;
import io.atomix.copycat.server.storage.snapshot.SnapshotWriter;

import java.util.HashMap;
import java.util.Map;

public class SpannerStateMachine extends StateMachine implements Snapshottable {

    private static final String LOG_TAG = "SPANNER_STATE_MACHINE";
    private Map<String, String> map = new HashMap<>();

    @Override
    public void snapshot(SnapshotWriter writer) {
        // Serialize the map to the snapshot
        writer.writeObject(map);
    }

    @Override
    public void install(SnapshotReader reader) {
        // Read the snapshotted map
        map = reader.readObject();
    }

    public void write(Commit<WriteCommand> commit) {
        try {
            LogUtils.debug(LOG_TAG, "Applying WriteCommand command.");
            Map<String, String> map = commit.operation().getWriteCommands();
            for (String key : map.keySet()) {
                map.put(key, map.get(key));
            }
        } finally {
            commit.close();
        }
    }

    public String read(Commit<ReadQuery> commit) {
        try {
            String key = commit.operation().key();
            LogUtils.debug(LOG_TAG, "Received Read Query for key:"+key);
            return map.get(key);
        } finally {
            commit.close();
        }
    }

    public void prepareCommit(Commit<PrepareCommitCommand> commit) {
        try {
            LogUtils.debug(LOG_TAG, "Applying PrepareCommit command.");
            // TODO
            return;
        } finally {
            commit.close();
        }
    }

    public void commit(Commit<CommitCommand> commit) {
        try {
            LogUtils.debug(LOG_TAG, "Applying Commit command.");
            // TODO
            return;
        } finally {
            commit.close();
        }
    }
}
