package com.lucid.comparison;

import com.lucid.test.PutCommand;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.StateMachine;

public class CopycatStateMachine extends StateMachine {

    public Object put(Commit<PutCommand> commit) {
        // Nothing to do
        return null;
    }
}
