/*
 * Copyright 2010-2017 Boxfuse GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.flywaydb.core.internal.dbsupport.exasol;

import org.flywaydb.core.internal.dbsupport.JdbcTemplate;
import org.flywaydb.core.internal.dbsupport.Schema;
import org.flywaydb.core.internal.dbsupport.Table;
import org.flywaydb.core.internal.dbsupport.oracle.OracleTable;
import org.flywaydb.core.internal.util.logging.Log;
import org.flywaydb.core.internal.util.logging.LogFactory;

import java.sql.SQLException;
import java.util.List;

/**
 * Oracle implementation of Schema.
 */
public class ExasolSchema extends Schema<ExasolDbSupport> {
    private static final Log LOG = LogFactory.getLog(ExasolSchema.class);

    /**
     * Creates a new Oracle schema.
     *
     * @param jdbcTemplate The Jdbc Template for communicating with the DB.
     * @param dbSupport    The database-specific support.
     * @param name         The name of the schema.
     */
    public ExasolSchema(JdbcTemplate jdbcTemplate, ExasolDbSupport dbSupport, String name) {
        super(jdbcTemplate, dbSupport, name);
    }

    /**
     * Checks whether the schema is system, i.e. Oracle-maintained, or not.
     *
     * @return {@code true} if it is system, {@code false} if not.
     */
    protected boolean isSystem() throws SQLException {
        return "SYS".equalsIgnoreCase(name);
    }

    /**
     * Checks whether this schema is default for the current user.
     *
     * @return {@code true} if it is default, {@code false} if not.
     */
    protected boolean isDefaultSchemaForUser() throws SQLException {
        return name.equals(dbSupport.getCurrentUserName());
    }

    @Override
    protected boolean doExists() throws SQLException {
        return dbSupport.queryReturnsRows("SELECT * FROM SYS.EXA_SCHEMAS WHERE SCHEMA_NAME = ?", name);
    }

    @Override
    protected boolean doEmpty() throws SQLException {
        return !dbSupport.queryReturnsRows("SELECT * FROM SYS.EXA_ALL_OBJECTS where root_name = upper(?)", name);
    }

    @Override
    protected void doCreate() throws SQLException {
        jdbcTemplate.execute("CREATE SCHEMA " + dbSupport.quote(name) );
        jdbcTemplate.execute("GRANT RESOURCE TO " + dbSupport.quote(name));
    }

    @Override
    protected void doDrop() throws SQLException {
        jdbcTemplate.execute("DROP USER " + dbSupport.quote(name) + " CASCADE");
    }

    @Override
    protected void doClean() throws SQLException {
        throw new UnsupportedOperationException("TODO clean an exasol scchema");
    }

    @Override
    protected Table[] doAllTables() throws SQLException {
        String tablesQuery =
            "SELECT table_name FROM SYS.EXA_USER_TABLES where table_schema = upper(?)";

        List<String> tableNames = jdbcTemplate.queryForStringList(tablesQuery, name);

        Table[] tables = new Table[tableNames.size()];
        for (int i = 0; i < tableNames.size(); i++) {
            tables[i] = new OracleTable(jdbcTemplate, dbSupport, this, tableNames.get(i));
        }
        return tables;
    }

    @Override
    public Table getTable(String tableName) {
        return new ExasolTable(jdbcTemplate, dbSupport, this, tableName);
    }
}
