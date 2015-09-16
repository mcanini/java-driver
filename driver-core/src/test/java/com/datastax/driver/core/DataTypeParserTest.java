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
import static com.datastax.driver.core.DataType.*;
import static com.datastax.driver.core.DataTypeParser.parse;

@CassandraVersion(major = 3.0)
public class DataTypeParserTest extends CCMBridge.PerClassSingleNodeCluster {

    @Test(groups = "short")
    public void should_parse_native_types() {
        assertThat(parse("ascii", cluster, null, null, false)).isEqualTo(ascii());
        assertThat(parse("bigint", cluster, null, null, false)).isEqualTo(bigint());
        assertThat(parse("blob", cluster, null, null, false)).isEqualTo(blob());
        assertThat(parse("boolean", cluster, null, null, false)).isEqualTo(cboolean());
        assertThat(parse("counter", cluster, null, null, false)).isEqualTo(counter());
        assertThat(parse("decimal", cluster, null, null, false)).isEqualTo(decimal());
        assertThat(parse("double", cluster, null, null, false)).isEqualTo(cdouble());
        assertThat(parse("float", cluster, null, null, false)).isEqualTo(cfloat());
        assertThat(parse("inet", cluster, null, null, false)).isEqualTo(inet());
        assertThat(parse("int", cluster, null, null, false)).isEqualTo(cint());
        assertThat(parse("text", cluster, null, null, false)).isEqualTo(text());
        assertThat(parse("varchar", cluster, null, null, false)).isEqualTo(varchar());
        assertThat(parse("timestamp", cluster, null, null, false)).isEqualTo(timestamp());
        assertThat(parse("date", cluster, null, null, false)).isEqualTo(date());
        assertThat(parse("time", cluster, null, null, false)).isEqualTo(time());
        assertThat(parse("uuid", cluster, null, null, false)).isEqualTo(uuid());
        assertThat(parse("varint", cluster, null, null, false)).isEqualTo(varint());
        assertThat(parse("timeuuid", cluster, null, null, false)).isEqualTo(timeuuid());
        assertThat(parse("tinyint", cluster, null, null, false)).isEqualTo(tinyint());
        assertThat(parse("smallint", cluster, null, null, false)).isEqualTo(smallint());
    }

    @Test(groups = "short")
    public void should_ignore_whitespace() {
        assertThat(parse("  int  ", cluster, null, null, false)).isEqualTo(cint());
        assertThat(parse("  set < bigint > ", cluster, null, null, false)).isEqualTo(set(bigint()));
        assertThat(parse("  map  <  date  ,  timeuuid  >  ", cluster, null, null, false)).isEqualTo(map(date(), timeuuid()));
    }

    @Test(groups = "short")
    public void should_ignore_case() {
        assertThat(parse("INT", cluster, null, null, false)).isEqualTo(cint());
        assertThat(parse("SET<BIGint>", cluster, null, null, false)).isEqualTo(set(bigint()));
        assertThat(parse("FROZEN<mAp<Date,Tuple<timeUUID>>>", cluster, null, null, false)).isEqualTo(map(date(), cluster.getMetadata().newTupleType(timeuuid()), true));
    }

    @Test(groups = "short")
    public void should_parse_collection_types() {
        assertThat(parse("list<int>", cluster, null, null, false)).isEqualTo(list(cint()));
        assertThat(parse("set<bigint>", cluster, null, null, false)).isEqualTo(set(bigint()));
        assertThat(parse("map<date,timeuuid>", cluster, null, null, false)).isEqualTo(map(date(), timeuuid()));
    }

    @Test(groups = "short")
    public void should_parse_frozen_collection_types() {
        assertThat(parse("frozen<list<int>>", cluster, null, null, false)).isEqualTo(list(cint(), true));
        assertThat(parse("frozen<set<bigint>>", cluster, null, null, false)).isEqualTo(set(bigint(), true));
        assertThat(parse("frozen<map<date,timeuuid>>", cluster, null, null, false)).isEqualTo(map(date(), timeuuid(), true));
    }

    @Test(groups = "short")
    public void should_parse_nested_collection_types() {
        assertThat(parse("list<list<int>>", cluster, null, null, false)).isEqualTo(list(list(cint())));
        assertThat(parse("set<list<frozen<map<bigint,varchar>>>>", cluster, null, null, false)).isEqualTo(set(list(map(bigint(), varchar(), true))));
    }

    @Test(groups = "short")
    public void should_parse_tuple_types() {
        assertThat(parse("tuple<int,list<text>>", cluster, null, null, false)).isEqualTo(cluster.getMetadata().newTupleType(cint(), list(text())));
    }

    @Test(groups = "short")
    public void should_parse_user_defined_types() throws ExecutionException, InterruptedException {
        Metadata metadata = cluster.getMetadata();
        KeyspaceMetadata keyspaceMetadata = metadata.getKeyspace(this.keyspace);
        UserType a = keyspaceMetadata.getUserType("\"A\"");
        assertThat(parse("\"A\"", cluster, keyspace, null, false)).isEqualTo(a);
    }

    @Override
    protected Collection<String> getTableDefinitions() {
        return Lists.newArrayList(
            String.format("CREATE TYPE %s.\"A\" (f1 int)", keyspace)
        );
    }
    
}
