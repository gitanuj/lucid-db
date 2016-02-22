import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.StateMachine;

import java.util.HashMap;
import java.util.Map;

public class MapStateMachine extends StateMachine {
    private final Map<Object, Commit<PutCommand>> map = new HashMap<>();

    public Object put(Commit<PutCommand> commit) {
        try {
            map.put(commit.operation().key(), commit);
            return commit.operation().value();
        } finally {
            commit.close();
        }
    }

    public Object get(Commit<GetQuery> commit) {
        try {
            // Get the commit value and return the operation value if available.
            Commit<PutCommand> value = map.get(commit.operation().key());
            return value != null ? value.operation().value() : null;
        } finally {
            // Close the query commit once complete to release it back to the internal commit pool.
            // Failing to do so will result in warning messages.
            commit.close();
        }
    }

    public Object remove(Commit<RemoveCommand> commit) {
        try {
            // Remove the commit with the given key.
            Commit<PutCommand> value = map.remove(commit.operation().key());

            // If a commit with the given key existed, get the result and then close the commit from the log.
            if (value != null) {
                Object result = value.operation().value();
                value.close();
                return result;
            }
            return null;
        } finally {
            // Finally, close the remove commit.
            commit.close();
        }
    }
}