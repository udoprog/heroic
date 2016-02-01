package com.spotify.heroic.metric.bigtable.api;

import com.google.bigtable.v1.ReadModifyWriteRule;

import java.util.List;

import lombok.Data;

@Data
public class ReadModifyWriteRules {
    private final List<ReadModifyWriteRule> rules;
}
