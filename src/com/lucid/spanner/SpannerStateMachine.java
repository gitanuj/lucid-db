package com.lucid.spanner;

import io.atomix.copycat.server.Snapshottable;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.storage.snapshot.SnapshotReader;
import io.atomix.copycat.server.storage.snapshot.SnapshotWriter;

import java.util.HashMap;
import java.util.Map;

public class SpannerStateMachine extends StateMachine implements Snapshottable {

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
}
