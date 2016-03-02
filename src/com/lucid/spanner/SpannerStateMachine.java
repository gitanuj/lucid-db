package com.lucid.spanner;

import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.Snapshottable;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.storage.snapshot.SnapshotReader;
import io.atomix.copycat.server.storage.snapshot.SnapshotWriter;

import java.util.HashMap;
import java.util.Map;

public class SpannerStateMachine extends StateMachine implements Snapshottable {

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
            return map.get(key);
        } finally {
            commit.close();
        }
    }

    public void prepareCommit(Commit<PrepareCommitCommand> commit) {
        try {
            // TODO
            return;
        } finally {
            commit.close();
        }
    }

    public void commit(Commit<CommitCommand> commit) {
        try {
            // TODO
            return;
        } finally {
            commit.close();
        }
    }
}
