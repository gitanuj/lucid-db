import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.StateMachineExecutor;

import java.util.HashMap;
import java.util.Map;

public class MapStateMachine extends StateMachine {
    private final Map<Object, Object> map = new HashMap<>();

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