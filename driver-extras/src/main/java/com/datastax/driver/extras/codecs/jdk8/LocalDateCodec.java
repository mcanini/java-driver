/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.extras.codecs.jdk8;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidTypeException;

/**
 * {@link TypeCodec} that maps
 * {@link java.time.LocalDate} to CQL <code>date</code> columns.
 */
public class LocalDateCodec extends TypeCodec<LocalDate> {

    public static final LocalDateCodec instance = new LocalDateCodec();

    private static final LocalDate EPOCH = LocalDate.ofEpochDay(0);

    private LocalDateCodec() {
        super(DataType.date(), LocalDate.class);
    }

    @Override
    public ByteBuffer serialize(LocalDate value, ProtocolVersion protocolVersion) {
        if (value == null)
            return null;
        long days = ChronoUnit.DAYS.between(EPOCH, value);
        int unsigned = CodecUtils.fromSignedToUnsignedInt((int) days);
        return cint().serializeNoBoxing(unsigned, protocolVersion);
    }

    @Override
    public LocalDate deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        if (bytes == null || bytes.remaining() == 0)
            return null;
        int unsigned = cint().deserializeNoBoxing(bytes, protocolVersion);
        int signed = CodecUtils.fromUnsignedToSignedInt(unsigned);
        return EPOCH.plusDays(signed);
    }

    @Override
    public String format(LocalDate value) {
        if (value == null)
            return "NULL";
        return "'" + ISO_LOCAL_DATE.format(value) + "'";
    }

    @Override
    public LocalDate parse(String value) {
        if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
            return null;

        // single quotes are optional for long literals, mandatory for date patterns
        // strip enclosing single quotes, if any
        if (ParseUtils.isQuoted(value))
            value = ParseUtils.unquote(value);

        if (ParseUtils.isLongLiteral(value)) {
            long raw;
            try {
                raw = Long.parseLong(value);
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse date value from \"%s\"", value));
            }
            int days;
            try {
                days = CodecUtils.fromCqlDateToDaysSinceEpoch(raw);
            } catch (IllegalArgumentException e) {
                throw new InvalidTypeException(String.format("Cannot parse date value from \"%s\"", value));
            }
            return EPOCH.plusDays(days);
        }

        try {
            return LocalDate.parse(value, ISO_LOCAL_DATE);
        } catch (RuntimeException e) {
            throw new InvalidTypeException(String.format("Cannot parse date value from \"%s\"", value));
        }
    }

}
