package com.orchestrator.postgres.serialization;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A deliberately minimal JSON object reader/writer, scoped to exactly what
 * {@link HandWrittenJsonEventSerializer} needs: flat objects (no nesting, no
 * arrays) with string/number/null values. This is not a general-purpose JSON
 * library and must never be treated as one — see
 * {@link HandWrittenJsonEventSerializer}'s javadoc for why it exists at all
 * instead of using a real JSON library.
 */
final class SimpleJson {

    private SimpleJson() {
    }

    static String escape(String s) {
        if (s == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> sb.append(c);
            }
        }
        return sb.toString();
    }

    /** Builder for a flat JSON object — every value written as either a quoted string or the literal {@code null}. */
    static final class Writer {
        private final StringBuilder sb = new StringBuilder("{");
        private boolean first = true;

        Writer field(String key, String value) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            sb.append('"').append(key).append("\":");
            if (value == null) {
                sb.append("null");
            } else {
                sb.append('"').append(escape(value)).append('"');
            }
            return this;
        }

        Writer field(String key, int value) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            sb.append('"').append(key).append("\":").append(value);
            return this;
        }

        String build() {
            return sb.append('}').toString();
        }
    }

    /**
     * Parses a flat JSON object into a string-valued map. Every value comes
     * back as its raw string form (or {@code null} for a JSON {@code null});
     * the caller is responsible for parsing ints/UUIDs/Instants out of that
     * string as appropriate for the field in question.
     */
    static Map<String, String> parseFlatObject(String json) {
        Map<String, String> result = new LinkedHashMap<>();
        String body = json.trim();
        if (!body.startsWith("{") || !body.endsWith("}")) {
            throw new IllegalArgumentException("Not a flat JSON object: " + json);
        }
        body = body.substring(1, body.length() - 1);
        int i = 0;
        int n = body.length();
        while (i < n) {
            while (i < n && (Character.isWhitespace(body.charAt(i)) || body.charAt(i) == ',')) {
                i++;
            }
            if (i >= n) {
                break;
            }
            if (body.charAt(i) != '"') {
                throw new IllegalArgumentException("Expected a quoted key at index " + i + " in: " + json);
            }
            int keyStart = ++i;
            while (body.charAt(i) != '"') {
                i++;
            }
            String key = body.substring(keyStart, i);
            i++; // closing quote
            while (i < n && Character.isWhitespace(body.charAt(i))) {
                i++;
            }
            if (body.charAt(i) != ':') {
                throw new IllegalArgumentException("Expected ':' at index " + i + " in: " + json);
            }
            i++;
            while (i < n && Character.isWhitespace(body.charAt(i))) {
                i++;
            }

            String value;
            if (body.charAt(i) == '"') {
                i++;
                StringBuilder valueBuilder = new StringBuilder();
                while (body.charAt(i) != '"') {
                    if (body.charAt(i) == '\\') {
                        i++;
                        char escaped = body.charAt(i);
                        valueBuilder.append(switch (escaped) {
                            case 'n' -> '\n';
                            case 'r' -> '\r';
                            case 't' -> '\t';
                            default -> escaped;
                        });
                    } else {
                        valueBuilder.append(body.charAt(i));
                    }
                    i++;
                }
                value = valueBuilder.toString();
                i++; // closing quote
            } else if (body.startsWith("null", i)) {
                value = null;
                i += 4;
            } else {
                int valueStart = i;
                while (i < n && body.charAt(i) != ',') {
                    i++;
                }
                value = body.substring(valueStart, i).trim();
            }
            result.put(key, value);
        }
        return result;
    }
}
