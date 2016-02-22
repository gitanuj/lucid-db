import io.atomix.copycat.Command;

public class RemoveCommand implements Command<Object> {
    private final Object key;

    public RemoveCommand(Object key) {
        this.key = key;
    }

    @Override
    public CompactionMode compaction() {
        return Command.CompactionMode.SEQUENTIAL;
    }

    public Object key() {
        return key;
    }
}
