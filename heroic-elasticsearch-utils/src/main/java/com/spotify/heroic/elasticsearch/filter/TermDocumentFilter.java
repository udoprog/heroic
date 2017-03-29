package com.spotify.heroic.elasticsearch.filter;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import lombok.Data;

@JsonTypeName("term")
@Data
public class TermDocumentFilter implements DocumentFilter {
    private final String key;
    private final String value;

    @JsonValue
    public Map<String, String> value() {
        return ImmutableMap.of(key, value);
    }
}
