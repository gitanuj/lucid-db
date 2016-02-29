package com.lucid.ycsb;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.text.MessageFormat;
import java.util.*;

public final class YCSBUtils {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private YCSBUtils() {
    }

    public static String createQualifiedKey(String table, String key) {
        return MessageFormat.format("{0}-{1}", table, key);
    }

    public static void fromJson(String value, Set<String> fields, Map<String, ByteIterator> result) throws IOException {
        JsonNode json = MAPPER.readTree(value);
        boolean checkFields = fields != null && !fields.isEmpty();
        for (Iterator<Map.Entry<String, JsonNode>> jsonFields = json.getFields();
             jsonFields.hasNext();
         /* increment in loop body */) {
            Map.Entry<String, JsonNode> jsonField = jsonFields.next();
            String name = jsonField.getKey();
            if (checkFields && fields.contains(name)) {
                continue;
            }
            JsonNode jsonValue = jsonField.getValue();
            if (jsonValue != null && !jsonValue.isNull()) {
                result.put(name, new StringByteIterator(jsonValue.asText()));
            }
        }
    }

    public static String toJson(Map<String, ByteIterator> values) throws IOException {
        ObjectNode node = MAPPER.createObjectNode();
        HashMap<String, String> stringMap = StringByteIterator.getStringMap(values);
        for (Map.Entry<String, String> pair : stringMap.entrySet()) {
            node.put(pair.getKey(), pair.getValue());
        }
        JsonFactory jsonFactory = new JsonFactory();
        Writer writer = new StringWriter();
        JsonGenerator jsonGenerator = jsonFactory.createJsonGenerator(writer);
        MAPPER.writeTree(jsonGenerator, node);
        return writer.toString();
    }

    public static Map<String, String> toCommandsMap(List<WriteObject> batch) {
        Map<String, String> commandsMap = new HashMap<>();
        for (WriteObject writeObject : batch) {
            commandsMap.put(writeObject.getKey(), writeObject.getValue());
        }
        return commandsMap;
    }
}
