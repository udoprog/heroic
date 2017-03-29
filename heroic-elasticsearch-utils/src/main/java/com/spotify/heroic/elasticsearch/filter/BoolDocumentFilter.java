package com.spotify.heroic.elasticsearch.filter;

import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.List;
import lombok.Data;

@JsonTypeName("bool")
@Data
public class BoolDocumentFilter implements DocumentFilter {
    private final List<DocumentFilter> should;
    private final List<DocumentFilter> must;
}
