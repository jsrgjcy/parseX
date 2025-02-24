package com.sucx.core;

import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.*;
import com.sucx.common.Constants;
import com.sucx.common.enums.OperatorType;
import com.sucx.common.exceptions.SqlParseException;
import com.sucx.common.model.TableInfo;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.log4j.Logger;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 目前presto 仅仅是用来查询，目前还没解析输入表
 * desc: 解析presto sql的输入表、字段
 *
 * @author scx
 * @create 2020/03/09
 */

public class PrestoSqlParse extends AbstractSqlParse {

    private static Logger log = Logger.getLogger(PrestoSqlParse.class);

    private HashSet<TableInfo> inputTables;
    private HashSet<TableInfo> outputTables;
    private HashSet<TableInfo> tempTables;

    /**
     * select 字段表达式中获取字段
     *
     * @param expression
     * @return
     */
    private String getColumn(Expression expression) throws SqlParseException {
        if (expression instanceof IfExpression) {
            IfExpression ifExpression = (IfExpression) expression;
            List<Expression> list = new ArrayList<>();
            list.add(ifExpression.getCondition());
            list.add(ifExpression.getTrueValue());
            ifExpression.getFalseValue().ifPresent(list::add);
            return getString(list);
        } else if (expression instanceof Identifier) {
            Identifier identifier = (Identifier) expression;
            return identifier.getValue();
        } else if (expression instanceof FunctionCall) {
            FunctionCall call = (FunctionCall) expression;
            StringBuilder columns = new StringBuilder();
            List<Expression> arguments = call.getArguments();
            int size = arguments.size();
            for (int i = 0; i < size; i++) {
                Expression exp = arguments.get(i);
                if (i == 0) {
                    columns.append(getColumn(exp));
                } else {
                    columns.append(getColumn(exp)).append(columnSplit);
                }
            }
            return columns.toString();
        } else if (expression instanceof ComparisonExpression) {
            ComparisonExpression compare = (ComparisonExpression) expression;
            return getString(compare.getLeft(), compare.getRight());
        } else if (expression instanceof Literal || expression instanceof ArithmeticUnaryExpression) {
            return "";
        } else if (expression instanceof Cast) {
            Cast cast = (Cast) expression;
            return getColumn(cast.getExpression());
        } else if (expression instanceof DereferenceExpression) {
            DereferenceExpression reference = (DereferenceExpression) expression;
            return reference.toString();
        } else if (expression instanceof ArithmeticBinaryExpression) {
            ArithmeticBinaryExpression binaryExpression = (ArithmeticBinaryExpression) expression;
            return getString(binaryExpression.getLeft(), binaryExpression.getRight());
        } else if (expression instanceof SearchedCaseExpression) {
            SearchedCaseExpression caseExpression = (SearchedCaseExpression) expression;
            List<Expression> exps = caseExpression.getWhenClauses().stream().map(whenClause -> (Expression) whenClause).collect(Collectors.toList());
            caseExpression.getDefaultValue().ifPresent(exps::add);
            return getString(exps);
        } else if (expression instanceof WhenClause) {
            WhenClause whenClause = (WhenClause) expression;
            return getString(whenClause.getOperand(), whenClause.getResult());
        } else if (expression instanceof LikePredicate) {
            LikePredicate likePredicate = (LikePredicate) expression;
            return likePredicate.getValue().toString();
        } else if (expression instanceof InPredicate) {
            InPredicate predicate = (InPredicate) expression;
            return predicate.getValue().toString();
        } else if (expression instanceof SubscriptExpression) {
            SubscriptExpression subscriptExpression = (SubscriptExpression) expression;
            return getColumn(subscriptExpression.getBase());
        } else if (expression instanceof LogicalBinaryExpression) {
            LogicalBinaryExpression logicExp = (LogicalBinaryExpression) expression;
            return getString(logicExp.getLeft(), logicExp.getRight());
        } else if (expression instanceof IsNullPredicate) {
            IsNullPredicate isNullExp = (IsNullPredicate) expression;
            return getColumn(isNullExp.getValue());
        } else if (expression instanceof IsNotNullPredicate) {
            IsNotNullPredicate notNull = (IsNotNullPredicate) expression;
            return getColumn(notNull.getValue());
        } else if (expression instanceof CoalesceExpression) {
            CoalesceExpression coalesce = (CoalesceExpression) expression;
            return getString(coalesce.getOperands());
        }
        throw new SqlParseException("无法识别的表达式:" + expression.getClass().getName());
        //   return expression.toString();
    }


    private String getString(Expression... exps) throws SqlParseException {
        return getString(Arrays.stream(exps).collect(Collectors.toList()));
    }

    private String getString(List<Expression> exps) throws SqlParseException {
        StringBuilder builder = new StringBuilder();
        for (Expression exp : exps) {
            builder.append(getColumn(exp)).append(columnSplit);
        }
        return builder.toString();
    }


    /**
     * node 节点的遍历
     *
     * @param node
     */
    private void checkNode(Node node) throws SqlParseException {
        if (node instanceof QuerySpecification) {
            QuerySpecification query = (QuerySpecification) node;
            query.getLimit().ifPresent(limit -> limitStack.push(limit));
            loopNode(query.getChildren());
        } else if (node instanceof TableSubquery) {
            loopNode(node.getChildren());
        } else if (node instanceof AliasedRelation) {
            AliasedRelation alias = (AliasedRelation) node;
            String value = alias.getAlias().getValue();
            if (alias.getChildren().size() == 1 && alias.getChildren().get(0) instanceof Table) {
                Table table = (Table) alias.getChildren().get(0);
                tableAliaMap.put(value, table.getName().toString());
            } else {
                tempTables.add(buildTableInfo(value, OperatorType.READ));
            }
            loopNode(node.getChildren());
        } else if (node instanceof Query || node instanceof SubqueryExpression
                || node instanceof Union || node instanceof With
                || node instanceof LogicalBinaryExpression || node instanceof InPredicate) {
            loopNode(node.getChildren());

        } else if (node instanceof Join) {
            hasJoin = true;
            loopNode(node.getChildren());
        }
        //基本都是where条件，过滤掉，如果需要，可以调用getColumn解析字段
        else if (node instanceof LikePredicate || node instanceof NotExpression
                || node instanceof IfExpression
                || node instanceof ComparisonExpression || node instanceof GroupBy
                || node instanceof OrderBy || node instanceof Identifier
                || node instanceof InListExpression || node instanceof DereferenceExpression
                || node instanceof IsNotNullPredicate || node instanceof IsNullPredicate
                || node instanceof FunctionCall) {
            print(node.getClass().getName());

        } else if (node instanceof WithQuery) {
            WithQuery withQuery = (WithQuery) node;
            tempTables.add(buildTableInfo(withQuery.getName().getValue(), OperatorType.READ));
            loopNode(withQuery.getChildren());
        } else if (node instanceof Table) {
            Table table = (Table) node;
            inputTables.add(buildTableInfo(table.getName().toString(), OperatorType.READ));
            loopNode(table.getChildren());
        } else if (node instanceof Select) {
            Select select = (Select) node;
            List<SelectItem> selectItems = select.getSelectItems();
            HashSet<FieldSchema> columns = new HashSet<>();
            for (SelectItem item : selectItems) {
                if (item instanceof SingleColumn) {
//                    getColumn(((SingleColumn) item).getExpression())
                    columns.add(new FieldSchema(getColumn(((SingleColumn) item).getExpression()), "", ""));
                } else if (item instanceof AllColumns) {
//                    columns.add(item.toString());
                    columns.add(new FieldSchema(item.toString(), "", ""));
                } else {
                    throw new SqlParseException("unknow column type:" + item.getClass().getName());
                }
            }
            columnsStack.push(columns);

        } else {
            throw new SqlParseException("unknow node type:" + node.getClass().getName());
        }
    }


    private void loopNode(List<? extends Node> children) throws SqlParseException {
        for (Node node : children) {
            this.checkNode(node);
        }
    }

    /**
     * statement 过滤 只识别select 语句
     *
     * @param statement
     * @throws SqlParseException
     */
    private void check(Statement statement) throws SqlParseException {
        if (statement instanceof Query) {
            Query query = (Query) statement;
            List<Node> children = query.getChildren();
            for (Node child : children) {
                checkNode(child);
            }
        } else if (statement instanceof Use) {
            Use use = (Use) statement;
            this.currentDb = use.getSchema().getValue();
        } else if (statement instanceof ShowColumns) {
            ShowColumns show = (ShowColumns) statement;
            String allName = show.getTable().toString().replace("hive.", "");
            inputTables.add(buildTableInfo(allName, OperatorType.READ));
        } else if (statement instanceof ShowTables) {
            ShowTables show = (ShowTables) statement;
            QualifiedName qualifiedName = show.getSchema().orElseThrow(() -> new SqlParseException("unkonw table name or db name" + statement.toString()));
            String allName = qualifiedName.toString().replace("hive.", "");
            if (allName.contains(Constants.POINT)) {
                allName += Constants.POINT + "*";
            }
            inputTables.add(buildTableInfo(allName, OperatorType.READ));

        } else {
            throw new SqlParseException("sorry,only support read statement,unSupport statement:" + statement.getClass().getName());
        }
    }


    @Override
    protected Tuple3<HashSet<TableInfo>, HashSet<TableInfo>, HashSet<TableInfo>> parseInternal(String sqlText) throws SqlParseException {
        this.inputTables = new HashSet<>();
        this.outputTables = new HashSet<>();
        this.tempTables = new HashSet<>();
        try {
            check(new SqlParser().createStatement(sqlText, new ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL)));
        } catch (ParsingException e) {
            throw new SqlParseException("parse sql exception:" + e.getMessage(), e);
        }
        return new Tuple3<>(inputTables, outputTables, tempTables);
    }
}
