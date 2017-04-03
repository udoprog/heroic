package com.spotify.heroic.lib.httpcore;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import lombok.Data;

@Data
public final class Accept {
    public static Pattern SECTION_SEPARATOR = Pattern.compile(",");
    public static Pattern ELEMENT_SEPARATOR = Pattern.compile(";");

    public static final Accept ANY = new Accept(ImmutableList.of());

    private final List<WeightedMimeType> mimeTypes;

    public static Accept parse(final CharSequence input) {
        final String[] sections = SECTION_SEPARATOR.split(input);

        if (sections.length == 0) {
            throw new IllegalArgumentException(input.toString());
        }

        final List<WeightedMimeType> mimeTypes = new ArrayList<>();

        for (final String section : sections) {
            final String[] parts = ELEMENT_SEPARATOR.split(section);

            final MimeType mimeType = MimeType.parse(parts[0].trim());
            float q = 1.0f;
            final List<NameValuePair> extensions = new ArrayList<>();

            for (int i = 1; i < parts.length; i++) {
                NameValuePair pair = NameValuePair.parse(parts[i]);

                if ("q".equals(pair.getName())) {
                    q = Float.parseFloat(pair.getValue());
                    continue;
                }

                extensions.add(pair);
            }

            mimeTypes.add(new WeightedMimeType(mimeType, q, extensions));
        }

        if (mimeTypes.size() == 1) {
            final WeightedMimeType first = mimeTypes.get(0);

            if (first.getMimeType().isWildcard() && first.getQ() == 1.0f &&
                first.getExtensions().isEmpty()) {
                return ANY;
            }
        }

        return new Accept(mimeTypes);
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();

        final Iterator<WeightedMimeType> it = mimeTypes.iterator();

        appendWeightedMimeType(builder, it.next());

        while (it.hasNext()) {
            builder.append(", ");
            appendWeightedMimeType(builder, it.next());
        }

        return builder.toString();
    }

    private void appendWeightedMimeType(
        final StringBuilder builder, final WeightedMimeType weighted
    ) {
        builder.append(weighted.getMimeType().toString());

        if (weighted.getQ() < 1.0f) {
            builder.append("; q=").append(weighted.getQ());
        }

        weighted.getExtensions().forEach(param -> {
            builder.append("; ").append(param.getName()).append("=").append(param.getValue());
        });
    }

    public boolean isAny() {
        if (this == ANY) {
            return true;
        }

        if (mimeTypes.isEmpty()) {
            return true;
        }

        if (mimeTypes.size() == 1) {
            final WeightedMimeType first = mimeTypes.get(0);

            if (first.getMimeType().isWildcard()) {
                return true;
            }
        }

        return false;
    }

    public Optional<MimeType> matchBest(final List<MimeType> produces) {
        if (isAny()) {
            return produces.stream().findFirst();
        }

        // TODO: take weight into account
        for (final MimeType mime : produces) {
            for (final WeightedMimeType weighted : mimeTypes) {
                if (weighted.mimeType.matches(mime)) {
                    return Optional.of(mime);
                }
            }
        }

        return Optional.empty();
    }

    @Data
    public static class WeightedMimeType {
        private final MimeType mimeType;
        private final float q;
        private final List<NameValuePair> extensions;
    }
}
