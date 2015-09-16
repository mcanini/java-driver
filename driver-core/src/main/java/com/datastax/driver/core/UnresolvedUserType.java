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
package com.datastax.driver.core;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import com.datastax.driver.core.exceptions.UnresolvedUserTypeException;

import static com.datastax.driver.core.Metadata.escapeId;

/**
 * An unresolved user-defined type.
 * Unresolved user-defined types can appear in the metadata when
 * a column or a field references a user-defined type whose definition
 * cannot be found in the current metadata.
 *
 * @since 3.0.0
 */
class UnresolvedUserType extends UserType {

    private final AtomicReference<UserType> resolved = new AtomicReference<UserType>(null);

    private final Metadata metadata;

    UnresolvedUserType(String keyspaceName, String typeName, Cluster cluster) {
        super(keyspaceName, typeName, Collections.<Field>emptyList(),
            cluster.getConfiguration().getProtocolOptions().getProtocolVersion(),
            cluster.getConfiguration().getCodecRegistry());
        this.metadata = cluster.getMetadata();
    }

    @Override
    public int size() {
        resolve();
        return resolved.get().size();
    }

    @Override
    public boolean contains(String name) {
        resolve();
        return resolved.get().contains(name);
    }

    @Override
    public Iterator<Field> iterator() {
        resolve();
        return resolved.get().iterator();
    }

    @Override
    public Collection<String> getFieldNames() {
        resolve();
        return resolved.get().getFieldNames();
    }

    @Override
    public DataType getFieldType(String name) {
        resolve();
        return resolved.get().getFieldType(name);
    }

    @Override
    public String exportAsString() {
        resolve();
        return resolved.get().exportAsString();
    }

    @Override
    public String asCQLQuery() {
        resolve();
        return resolved.get().asCQLQuery();
    }

    @Override
    public int hashCode() {
        resolve();
        return resolved.get().hashCode();
    }

    @Override
    Field[] getFields() {
        resolve();
        return resolved.get().getFields();
    }

    @Override
    Map<String, int[]> getFieldIndicesByName() {
        resolve();
        return resolved.get().getFieldIndicesByName();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof UserType))
            return false;
        resolve();
        return resolved.get().equals(o);
    }

    private void resolve() {
        if (resolved.get() == null) {
            UserType userType = null;
            KeyspaceMetadata keyspace = metadata.getKeyspace(escapeId(getKeyspace()));
            if (keyspace != null) {
                userType = keyspace.getUserType(escapeId(getTypeName()));
            }
            if (userType == null)
                throw new UnresolvedUserTypeException(getKeyspace(), getTypeName());
            resolved.compareAndSet(null, userType);
        }
    }

}
