package org.janusgraph.diskstorage.dynamo;

import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

/**
 * A reference to a DynamoTable.  Wrapped to handle closing the dynamo connection in the table and returning
 * it to the connection pool.
 */
class DynamoTable implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(DynamoTable.class);

    private final DynamoConnection dynamo;
    private final Table table;

    /**
     * This should only be called by {@link DynamoTableManager}.  Use this constructor when the table is already
     * known to exist.
     * @param pool connection pool
     * @param tableName name of the table
     * @throws BackendException if we cannot get a connection to Dynamo
     */
    DynamoTable(ConnectionPool pool, String tableName) throws BackendException {
        // We intentionally don't close this connection because we're using it with the table.  Closing the table
        // will close the connection.
        dynamo = pool.getConnection();
        table = dynamo.get().getTable(tableName);
    }

    /**
     * This should only be called by {@link DynamoTableManager}.  Use this constructor when you don't know if the
     * table exists.
     * @param pool connection pool
     * @param conf configuration object
     * @param tableDef definition of the table
     * @throws BackendException if the table can't be created or we cannot get a connection to Dynamo
     */
    DynamoTable(ConnectionPool pool, Configuration conf, CreateTableRequest tableDef) throws BackendException {
        // We intentionally don't close this connection because we're using it with the table.  Closing the table
        // will close the connection.
        dynamo = pool.getConnection();
        TableCollection<ListTablesResult> existingTables =
            dynamo.get().listTables(conf.get(Utils.DYNAMO_TABLE_BASE));
        for (Table maybe : existingTables) {
            if (maybe.getTableName().equals(tableDef.getTableName())) {
                table = maybe;
                return;
            }
        }
        // We didn't find it in the list, so we need to create it.
        LOG.info("Unable to find table " + tableDef.getTableName() + ", will create it");
        try {
            table = dynamo.get().createTable(tableDef);
            table.waitForActive();
        } catch (Exception e) {
            throw new TemporaryBackendException("Unable to create table", e);
        }
    }

    Table get() {
        return table;
    }


    @Override
    public void close() {
        dynamo.close();
    }
}
