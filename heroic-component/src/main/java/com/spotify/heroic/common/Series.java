/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.spotify.heroic.grammar.DSL;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;

import static com.google.common.base.Preconditions.checkNotNull;

@ToString(of = {"key", "tags"})
public class Series implements Comparable<Series> {
    static final HashFunction HASH_FUNCTION = Hashing.murmur3_128();
    static final SortedMap<String, String> EMPTY_TAGS = ImmutableSortedMap.of();
    static final Joiner COMMA = Joiner.on(", ");
    static final Joiner TAGS_JOINER = Joiner.on(", ");
    static final Series EMPTY =
        new Series(Optional.empty(), EMPTY_TAGS, Optional.empty(), Optional.empty());

    private final InternalScope internalScope = new InternalScope();

    final Optional<String> key;
    final SortedMap<String, String> tags;
    final Optional<SortedSet<String>> identity;
    final Optional<SortedSet<String>> scope;
    final HashCode hashCode;

    /**
     * Package-private constructor to avoid invalid inputs.
     *
     * @param key The key of the time series.
     * @param tags The tags of the time series.
     */
    @JsonCreator
    Series(
        @JsonProperty("key") Optional<String> key,
        @JsonProperty("tags") SortedMap<String, String> tags,
        @JsonProperty("identity") Optional<SortedSet<String>> identity,
        @JsonProperty("scope") Optional<SortedSet<String>> scope
    ) {
        this.key = key;
        this.tags = checkNotNull(tags, "tags");
        this.identity = identity;
        this.scope = scope;
        this.hashCode = generateHash();
    }

    public Optional<String> getKey() {
        return key;
    }

    public SortedMap<String, String> getTags() {
        return tags;
    }

    @JsonGetter("identity")
    final Optional<SortedSet<String>> getIdentitySet() {
        return identity;
    }

    @JsonGetter("scope")
    final Optional<SortedSet<String>> getScopeSet() {
        return scope;
    }

    public Scope getScope() {
        return internalScope;
    }

    public Identity getIdentity() {
        return new Identity();
    }

    @JsonIgnore
    public HashCode getHashCode() {
        return hashCode;
    }

    public String hash() {
        return hashCode.toString();
    }

    @Override
    public int hashCode() {
        return hashCode.asInt();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof Series) {
            final Series o = (Series) obj;
            return hashCode.equals(o.hashCode) && key.equals(o.key) && tags.equals(o.tags);
        }

        return false;
    }

    @Override
    public int compareTo(final Series other) {
        if (key.isPresent() && other.key.isPresent()) {
            final int k = key.get().compareTo(other.key.get());

            if (k != 0) {
                return k;
            }
        } else {
            if (!key.isPresent()) {
                return -1;
            }

            if (!other.key.isPresent()) {
                return 1;
            }
        }

        final Iterator<String> a =
            identity.map(Iterable::iterator).orElseGet(() -> tags.keySet().iterator());
        final Iterator<String> b =
            other.identity.map(Iterable::iterator).orElseGet(() -> tags.keySet().iterator());

        while (a.hasNext() && b.hasNext()) {
            final String ak = a.next();
            final String bk = b.next();

            final int kc = ak.compareTo(bk);

            if (kc != 0) {
                return kc;
            }

            final String av = tags.get(ak);
            final String bv = other.tags.get(ak);
            int kv = av.compareTo(bv);

            if (kv != 0) {
                return kv;
            }
        }

        if (a.hasNext()) {
            return 1;
        }

        if (b.hasNext()) {
            return -1;
        }

        return 0;
    }

    public String toDSL() {
        final Iterator<String> tags = this.tags
            .entrySet()
            .stream()
            .map(e -> DSL.dumpString(e.getKey()) + "=" + DSL.dumpString(e.getValue()))
            .iterator();

        final StringBuilder builder = new StringBuilder();

        key.ifPresent(key -> {
            builder.append(DSL.dumpString(key)).append(" ");
        });

        builder.append("{").append(TAGS_JOINER.join(tags)).append("} ").append(scopeDSL());
        return builder.toString();
    }

    public String scopeDSL() {
        return internalScope.toDSL();
    }

    private HashCode generateHash() {
        final Hasher hasher = HASH_FUNCTION.newHasher();

        key.ifPresent(key -> {
            hasher.putString(key, Charsets.UTF_8);
        });

        final Iterator<String> keys =
            identity.map(Iterable::iterator).orElseGet(() -> tags.keySet().iterator());

        while (keys.hasNext()) {
            final String key = keys.next();
            final String value = tags.get(key);

            hasher.putString(key, Charsets.UTF_8);
            hasher.putString(value, Charsets.UTF_8);
        }

        return hasher.hash();
    }

    private Iterable<Map.Entry<String, String>> entryIterable(final Iterable<String> keys) {
        return Iterables.transform(keys, key -> {
            final String value = tags.get(key);
            return Pair.of(key, value);
        });
    }

    public static Series empty() {
        return EMPTY;
    }

    public static Series of(Optional<String> key) {
        return new Series(key, EMPTY_TAGS, Optional.empty(), Optional.empty());
    }

    public static Series of(String key) {
        return new Series(Optional.of(key), EMPTY_TAGS, Optional.empty(), Optional.empty());
    }

    public static Series of(Optional<String> key, Map<String, String> tags) {
        return of(key, tags.entrySet().iterator(), Optional.empty());
    }

    public static Series of(String key, Map<String, String> tags) {
        return of(Optional.of(key), tags.entrySet().iterator(), Optional.empty());
    }

    public static Series of(
        String key, Map<String, String> tags, Optional<SortedSet<String>> scope
    ) {
        return of(Optional.of(key), tags.entrySet().iterator(), scope);
    }

    public static Series of(String key, Set<Map.Entry<String, String>> entries) {
        return of(Optional.of(key), entries.iterator(), Optional.empty());
    }

    public static Series of(
        String key, Iterator<Map.Entry<String, String>> tagPairs
    ) {
        return of(Optional.of(key), tagPairs, Optional.empty());
    }

    public static Series of(
        Optional<String> key, Iterator<Map.Entry<String, String>> tagPairs,
        Optional<SortedSet<String>> scope
    ) {
        final TreeMap<String, String> tags = new TreeMap<>();
        final Set<String> extraScopes = scope.map(HashSet::new).orElseGet(HashSet::new);

        while (tagPairs.hasNext()) {
            final Map.Entry<String, String> pair = tagPairs.next();
            final String tk = checkNotNull(pair.getKey());
            final String tv = pair.getValue();
            tags.put(tk, tv);
            extraScopes.remove(tk);
        }

        if (extraScopes.size() > 0) {
            throw new IllegalArgumentException(
                "scopes without a corresponding tags: " + extraScopes);
        }

        return new Series(key, tags, Optional.empty(), scope);
    }

    public static Scope scope(
        final String... pairs
    ) {
        if (pairs.length % 2 != 0) {
            throw new IllegalArgumentException("tags must come in pairs");
        }

        final List<Pair<String, String>> tags = new ArrayList<>();

        for (int i = 0; i < pairs.length; i += 2) {
            tags.add(Pair.of(pairs[i], pairs[i + 1]));
        }

        return new ListScope(tags);
    }

    public class Identity implements Iterable<Map.Entry<String, String>> {
        @Override
        public Iterator<Map.Entry<String, String>> iterator() {
            return identity.map(Series.this::entryIterable).orElseGet(tags::entrySet).iterator();
        }
    }

    public interface Scope extends Iterable<Map.Entry<String, String>>, Comparable<Scope> {
        @JsonValue
        default Map<String, String> scope() {
            return ImmutableMap.copyOf(this);
        }

        /**
         * Convert scope to DSL.
         *
         * @return a dsl representation of this scope
         */
        default String toDSL() {
            return "$scope(" + COMMA.join(Iterables.transform(this,
                p -> DSL.dumpString(p.getKey()) + "=" + DSL.dumpString(p.getValue()))) + ")";
        }

        @Override
        default int compareTo(@NonNull final Scope other) {
            final Iterator<Map.Entry<String, String>> a = iterator();
            final Iterator<Map.Entry<String, String>> b = other.iterator();

            while (a.hasNext() && b.hasNext()) {
                final Map.Entry<String, String> ae = a.next();
                final Map.Entry<String, String> be = b.next();

                final int key = ae.getKey().compareTo(be.getKey());

                if (key != 0) {
                    return key;
                }

                final int value = ae.getValue().compareTo(be.getValue());

                if (value != 0) {
                    return value;
                }
            }

            if (a.hasNext()) {
                return 1;
            }

            if (b.hasNext()) {
                return -1;
            }

            return 0;
        }

        static boolean equals(final Scope s, final Object o) {
            return o instanceof Scope && Iterables.elementsEqual(s, (Scope) o);
        }

        static int hashCode(Scope scope) {
            int hashCode = 1;

            for (Map.Entry<String, String> e : scope) {
                hashCode = 31 * hashCode + e.hashCode();
            }

            return hashCode;
        }

        static String hash(final Scope scope) {
            final Hasher hasher = HASH_FUNCTION.newHasher();

            int index = 0;

            for (final Map.Entry<String, String> e : scope) {
                hasher.putInt(index++);
                hasher.putString(e.getKey(), Charsets.UTF_8);
                hasher.putString(e.getValue(), Charsets.UTF_8);
            }

            return hasher.hash().toString();
        }

        String hash();
    }

    public class InternalScope implements Scope {
        @Override
        public Iterator<Map.Entry<String, String>> iterator() {
            return scope.map(Series.this::entryIterable).orElseGet(ImmutableList::of).iterator();
        }

        @Override
        public boolean equals(final Object o) {
            return Scope.equals(this, o);
        }

        @Override
        public int hashCode() {
            return Scope.hashCode(this);
        }

        @Override
        public String hash() {
            return Scope.hash(this);
        }
    }

    @RequiredArgsConstructor
    public static class ListScope implements Scope {
        private final List<? extends Map.Entry<String, String>> scope;

        @Override
        public Iterator<Map.Entry<String, String>> iterator() {
            return Iterators.transform(scope.iterator(), v -> v);
        }

        @Override
        public boolean equals(final Object o) {
            return Scope.equals(this, o);
        }

        @Override
        public int hashCode() {
            return Scope.hashCode(this);
        }

        @Override
        public String hash() {
            return Scope.hash(this);
        }
    }
}
