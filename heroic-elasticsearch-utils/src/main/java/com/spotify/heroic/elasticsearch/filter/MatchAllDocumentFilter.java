package com.spotify.heroic.elasticsearch.filter;

import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.Data;

@JsonTypeName("match_all")
@Data
public class MatchAllDocumentFilter implements DocumentFilter {
}
