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
import java.util.concurrent.ExecutionException;

import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import com.datastax.driver.core.utils.CassandraVersion;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.DataType.cint;
import static com.datastax.driver.core.DataType.list;
import static com.datastax.driver.core.DataType.map;
import static com.datastax.driver.core.DataType.set;

@CassandraVersion(major = 3.0)
public class UnresolvedUserTypeTest extends CCMBridge.PerClassSingleNodeCluster {

    @Test(groups = "short")
    public void should_resolve_nested_user_types() throws ExecutionException, InterruptedException {

        Metadata metadata = cluster.getMetadata();
        KeyspaceMetadata keyspaceMetadata = metadata.getKeyspace(this.keyspace);

        UserType a = keyspaceMetadata.getUserType("\"A\"");
        UserType b = keyspaceMetadata.getUserType("\"B\"");
        UserType c = keyspaceMetadata.getUserType("\"C\"");
        UserType d = keyspaceMetadata.getUserType("\"D\"");
        UserType e = keyspaceMetadata.getUserType("\"E\"");
        UserType f = keyspaceMetadata.getUserType("\"F\"");
        UserType g = keyspaceMetadata.getUserType("g");
        UserType h = keyspaceMetadata.getUserType("h");

        assertThat(a).hasField("f1", c);
        assertThat(b).hasField("f1", set(d));
        assertThat(c).hasField("f1", map(e, d));
        assertThat(d).hasField("f1", metadata.newTupleType(f, g, h));
        assertThat(e).hasField("f1", list(g));
        assertThat(f).hasField("f1", h);
        assertThat(g).hasField("f1", cint());
        assertThat(h).hasField("f1", cint());

        // each CREATE TYPE statement has triggered a partial schema refresh,
        // so nested UDTs are all unresolved

        assertThat(a.getFieldType("f1")).isInstanceOf(UnresolvedUserType.class);
        assertThat(b.getFieldType("f1").getTypeArguments().get(0)).isInstanceOf(UnresolvedUserType.class);
        assertThat(c.getFieldType("f1").getTypeArguments().get(0)).isInstanceOf(UnresolvedUserType.class);
        assertThat(c.getFieldType("f1").getTypeArguments().get(1)).isInstanceOf(UnresolvedUserType.class);
        assertThat(((TupleType)d.getFieldType("f1")).getComponentTypes().get(0)).isInstanceOf(UnresolvedUserType.class);
        assertThat(((TupleType)d.getFieldType("f1")).getComponentTypes().get(1)).isInstanceOf(UnresolvedUserType.class);
        assertThat(((TupleType)d.getFieldType("f1")).getComponentTypes().get(2)).isInstanceOf(UnresolvedUserType.class);
        assertThat(e.getFieldType("f1").getTypeArguments().get(0)).isInstanceOf(UnresolvedUserType.class);
        assertThat(f.getFieldType("f1")).isInstanceOf(UnresolvedUserType.class);

        // trigger a full schema refresh
        // nested UDTs should have all been resolved
        cluster.manager.submitSchemaRefresh(null, null, null, null).get();
        keyspaceMetadata = metadata.getKeyspace(this.keyspace);

        a = keyspaceMetadata.getUserType("\"A\"");
        b = keyspaceMetadata.getUserType("\"B\"");
        c = keyspaceMetadata.getUserType("\"C\"");
        d = keyspaceMetadata.getUserType("\"D\"");
        e = keyspaceMetadata.getUserType("\"E\"");
        f = keyspaceMetadata.getUserType("\"F\"");

        assertThat(a.getFieldType("f1")).isNotInstanceOf(UnresolvedUserType.class);
        assertThat(b.getFieldType("f1").getTypeArguments().get(0)).isNotInstanceOf(UnresolvedUserType.class);
        assertThat(c.getFieldType("f1").getTypeArguments().get(0)).isNotInstanceOf(UnresolvedUserType.class);
        assertThat(c.getFieldType("f1").getTypeArguments().get(1)).isNotInstanceOf(UnresolvedUserType.class);
        assertThat(((TupleType)d.getFieldType("f1")).getComponentTypes().get(0)).isNotInstanceOf(UnresolvedUserType.class);
        assertThat(((TupleType)d.getFieldType("f1")).getComponentTypes().get(1)).isNotInstanceOf(UnresolvedUserType.class);
        assertThat(((TupleType)d.getFieldType("f1")).getComponentTypes().get(2)).isNotInstanceOf(UnresolvedUserType.class);
        assertThat(e.getFieldType("f1").getTypeArguments().get(0)).isNotInstanceOf(UnresolvedUserType.class);
        assertThat(f.getFieldType("f1")).isNotInstanceOf(UnresolvedUserType.class);

    }

    @Override
    protected Collection<String> getTableDefinitions() {
        return Lists.newArrayList(
            /*
            Creates the following acyclic graph (edges directed upwards
            meaning "depends on"):

                H   G
               / \ /\
              F   |  E
               \ /  /
                D  /
               / \/
              B  C
                 |
                 A

             Topological sort order should be : GH,FE,D,CB,A
             */
            String.format("CREATE TYPE %s.h (f1 int)", keyspace),
            String.format("CREATE TYPE %s.g (f1 int)", keyspace),
            String.format("CREATE TYPE %s.\"F\" (f1 frozen<h>)", keyspace),
            String.format("CREATE TYPE %s.\"E\" (f1 frozen<list<g>>)", keyspace),
            String.format("CREATE TYPE %s.\"D\" (f1 frozen<tuple<\"F\",g,h>>)", keyspace),
            String.format("CREATE TYPE %s.\"C\" (f1 frozen<map<\"E\",\"D\">>)", keyspace),
            String.format("CREATE TYPE %s.\"B\" (f1 frozen<set<\"D\">>)", keyspace),
            String.format("CREATE TYPE %s.\"A\" (f1 frozen<\"C\">)", keyspace)
        );
    }
    
}
