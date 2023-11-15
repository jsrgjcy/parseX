package com.sucx.common.model;

import com.sucx.common.Constants;
import com.sucx.common.enums.OperatorType;
import com.sucx.common.util.Pair;
import com.sucx.common.util.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * desc:
 *
 * @author scx
 * @create 2020/02/26
 */
public class TableInfo {

    /**
     * 表名
     */
    private String name;

    /**
     * 库名
     */
    private String dbName;

    private OperatorType type;

    private Set<FieldSchema> columns;

    private String limit;

    private boolean selectAll;

    private boolean isDb;


    public TableInfo() {
    }


    public TableInfo(String dbName, OperatorType type) {
        this.dbName = dbName;
        this.type = type;
        this.isDb = true;
    }

    public TableInfo(String name, String dbName, OperatorType type, Set<FieldSchema> columns) {
        this.name = name;
        this.dbName = dbName;
        this.type = type;
        this.columns = new HashSet<>(columns);
        columns.clear();
        optimizeColumn();
    }

    public TableInfo(String dbAndTableName, OperatorType type, String defaultDb, HashSet<FieldSchema> columns) {
        if (dbAndTableName.contains(Constants.POINT)) {
            Pair<String, String> pair = StringUtils.getPointPair(dbAndTableName);
            this.name = pair.getRight();
            this.dbName = pair.getLeft();
        } else {
            this.name = dbAndTableName;
            this.dbName = defaultDb;
        }
        this.columns = new HashSet<>(columns);
        this.type = type;
        columns.clear();
        optimizeColumn();
    }

    public void setColumns(Set<FieldSchema> columns) {
        this.columns = columns;
    }

    public Set<FieldSchema> getColumns() {
        return columns;
    }

    private void optimizeColumn() {
        String dbAndName = this.dbName + Constants.POINT + this.name;
        this.columns = this.columns.stream().map(column -> {
            if (!selectAll && column.getName().endsWith("*")) {
                selectAll = true;
            }
            if (column.getName().contains(Constants.POINT)) {
                Pair<String, String> pair = StringUtils.getLastPointPair(column.getName());
                if (pair.getLeft().equals(dbAndName)) {
                    column.setName(pair.getRight());
                    return column;
                }
            }
            return column;
        }).collect(Collectors.toSet());
    }


    public boolean isDb() {
        return isDb;
    }

    public OperatorType getType() {
        return type;
    }

    public String getName() {
        return name;
    }


    public String getDbName() {
        return dbName;
    }

    public String getLimit() {
        return limit;
    }

    public void setLimit(String limit) {
        this.limit = limit;
    }


    public boolean isSelectAll() {
        return selectAll;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        if (isDb) {
            str.append("[库]").append(dbName).append("[").append(type.name()).append("]");
        } else {
            str.append("[表]").append(dbName).append(Constants.POINT).append(name).append("[").append(type.name()).append("]");
        }

        if (this.columns != null && this.columns.size() > 0) {
            str.append(" column[ ");
            this.columns.forEach(columns ->
            {
                String colStr = "(name:" + columns.getName() + ",type:" + columns.getType() + ",comment:" + columns.getComment() + ")";
                str.append(colStr).append(",");
            });
            str.append("]");
        }
        if (limit != null) {
            str.append(" limit[ ").append(limit).append(" ]");
        }
        return str.toString();
    }


    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TableInfo)) {
            return false;
        }

        TableInfo info = (TableInfo) obj;
        return this.dbName.equals(info.dbName) && this.name.equals(info.name) && this.type == info.type;
    }

    @Override
    public int hashCode() {
        if (this.name != null) {
            return this.dbName.hashCode() + this.name.hashCode() + this.type.hashCode();
        }
        return this.dbName.hashCode() + this.type.hashCode();
    }
}
