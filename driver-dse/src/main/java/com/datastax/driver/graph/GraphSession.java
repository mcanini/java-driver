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
package com.datastax.driver.graph;


import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;

public class GraphSession {
    private final Session session;

    public GraphSession(Session session) {
        this.session = session;
    }

    public GraphResultSet execute(GraphStatement gst) {
        if (!AbstractGraphStatement.checkStatement(gst)) {
            throw new InvalidQueryException("Invalid Graph Statement, you need to specify at least the keyspace containing the Graph data.");
        }
        // This is mandatory now to apply changes on the statement (payload).
        gst.configure();
        return new GraphResultSet(session.execute(gst));
    }

    public GraphResultSet execute(String query) {
        return new GraphResultSet(session.execute(new GraphStatement(query)));
    }

    public GraphResultSet execute(BoundGraphStatement bst) {
        if (!AbstractGraphStatement.checkStatement(bst)) {
            throw new InvalidQueryException("Invalid Graph Statement, you need to specify at least the keyspace containing the Graph data.");
        }
        // This is mandatory now to apply changes on the statement (payload).
        bst.configure();
        return new GraphResultSet(session.execute(bst.boundStatement()));
    }

    public PreparedGraphStatement prepare(GraphStatement gst) {
        return new PreparedGraphStatement(session.prepare(gst), gst);
    }

    public PreparedGraphStatement prepare(String query) {
        return new PreparedGraphStatement(session.prepare(new GraphStatement(query)));
    }

    public Session getSession() {
        return this.session;
    }
}