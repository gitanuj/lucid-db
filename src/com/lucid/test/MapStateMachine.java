package com.lucid.test;

import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.Snapshottable;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.storage.snapshot.SnapshotReader;
import io.atomix.copycat.server.storage.snapshot.SnapshotWriter;

import java.util.HashMap;
import java.util.Map;

public class MapStateMachine extends StateMachine implements Snapshottable {
    private Map<Object, Object> map = new HashMap<>();

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

    public Object put(Commit<PutCommand> commit) {
        try {
            Object key = commit.operation().key();
            Object value = commit.operation().value();

            map.put(key, value);
            return value;
        } finally {
            commit.close();
        }
    }

    public Object get(Commit<GetQuery> commit) {
        try {
            Object key = commit.operation().key();
            return map.get(key);
        } finally {
            commit.close();
        }
    }

    public Object remove(Commit<RemoveCommand> commit) {
        try {
            Object key = commit.operation().key();
            return map.remove(key);
        } finally {
            commit.close();
        }
    }
}