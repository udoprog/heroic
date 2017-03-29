package com.spotify.heroic.elasticsearch.client;

import java.util.Map;
import lombok.Data;

@Data
public class PutTemplate {
    private final String template;
    private final Map<String, Object> settings;
    private final Map<String, Map<String, Object>> mappings;
}
