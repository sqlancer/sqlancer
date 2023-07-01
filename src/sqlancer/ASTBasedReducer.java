package sqlancer;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitorAdapter;
import net.sf.jsqlparser.statement.select.*;
import sqlancer.common.query.Query;
import sqlancer.common.query.SQLQueryAdapter;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.function.BiConsumer;

class ExpressionTransformer {

    private static List<Expression> transformExpr(BinaryExpression expr) {
        List<Expression> candidates = new ArrayList<>();
        Expression lhs = (expr).getLeftExpression();
        Expression rhs = (expr).getRightExpression();
        candidates.add(lhs);
        candidates.add(rhs);
        return candidates;
    }

    private static List<Expression> transformExpr(Between expr) {
        Expression lhs = expr.getBetweenExpressionStart();
        Expression rhs = expr.getBetweenExpressionEnd();
        return List.of(lhs, rhs);
    }

    public static List<Expression> candidateExpressions(Expression expr) {

        // Queue<Expression> candidateExprQue = new PriorityQueue<>(Comparator.comparingInt(o ->
        // o.toString().length()));
        if (expr instanceof Parenthesis) {
            // try removing a pair of brackets.
            Parenthesis paren = (Parenthesis) expr;
            return List.of(paren.getExpression());
        } else if (expr instanceof BinaryExpression) {
            return transformExpr((BinaryExpression) expr);
        } else if (expr instanceof Between) {
            return transformExpr((Between) expr);
        } else if (expr instanceof LongValue) {
            LongValue longValue = (LongValue) expr;
            if (longValue.getValue() >= 1000) {
                return List.of(new NullValue(), new LongValue(10), new LongValue(0), new LongValue(1));
            }
            return new ArrayList<>();
        } else if (expr instanceof DoubleValue) {
            DoubleValue doubleValue = (DoubleValue) expr;
            double literal = doubleValue.getValue();
            if (String.valueOf(literal).length() <= 4) {
                return new ArrayList<>();
            }
            double roundedValue = Math.round(literal * 10.0) / 10.0;
            return List.of(new NullValue(), new DoubleValue(String.valueOf(roundedValue)));
        } else if (expr instanceof CaseExpression) {
            CaseExpression caseExpression = (CaseExpression) expr;
            return List.of(caseExpression.getSwitchExpression(), caseExpression.getElseExpression());
        } else
            return new ArrayList<>();
    }

}

@SuppressWarnings("unchecked")
public class ASTBasedReducer<G extends GlobalState<O, ?, C>, O extends DBMSSpecificOptions<?>, C extends SQLancerDBConnection>
        implements Reducer<G> {

    private final DatabaseProvider<G, O, C> provider;

    @SuppressWarnings("unused")
    private G state = null;
    private G newGlobalState = null;
    private Reproducer<G> reproducer = null;
    private int reduceTargetIndex;
    private Statement targetStatement;

    // statement after reduction.
    private List<Query<C>> reducedStatements;

    public ASTBasedReducer(DatabaseProvider<G, O, C> provider) {
        this.provider = provider;
    }

    private void updateStatements() {
        String queryString = targetStatement.toString();
        boolean couldAffectSchema = queryString.contains("CREATE TABLE") || queryString.contains("EXPLAIN");
        reducedStatements.set(reduceTargetIndex, (Query<C>) new SQLQueryAdapter(queryString, couldAffectSchema));
    }

    public <P> void expressionReduce(P parent, Expression subExpr, BiConsumer<P, Expression> setter) {
        boolean observeChange;
        do {
            observeChange = false;
            List<Expression> candidates = ExpressionTransformer.candidateExpressions(subExpr);
            for (Expression candidate : candidates) {
                try {
                    setter.accept(parent, candidate);
                    if (bugStillTriggers()) {
                        subExpr = candidate;
                        observeChange = true;
                    }
                } catch (Exception ignoredException) {
                }
            }
            setter.accept(parent, subExpr);
        } while (observeChange);
    }

    public <P, T> void listElementRemovingReduce(P parent, List<T> elms, BiConsumer<P, List<T>> setter) {
        // TODO: For AST-Reducer, is delta-debugging needed ? Or just use the naive approach ?
        boolean observeChange;
        do {
            observeChange = false;
            for (int i = elms.size() - 1; i >= 0; i--) {
                if (elms.size() == 1) {
                    // At least one element should be kept.
                    break;
                }
                List<T> reducedElms = new ArrayList<>(elms);
                reducedElms.subList(i, i + 1).clear();
                setter.accept(parent, reducedElms);
                try {
                    if (bugStillTriggers()) {
                        elms = reducedElms;
                        observeChange = true;
                    }
                } catch (Exception e) {
                    System.out.println("An error occurred when trying executing reduced statements");
                    e.printStackTrace();
                }
            }
            ;
            setter.accept(parent, elms);
        } while (observeChange);

    }

    ExpressionVisitorAdapter expressionReducerVisitor = new ExpressionVisitorAdapter() {

        @Override
        public void visit(InExpression expr) {
            Expression rhs = expr.getRightExpression();

            if (rhs instanceof SubSelect) {
                SubSelect subSelect = (SubSelect) rhs;
                subSelect.getSelectBody().accept(selectReducerVisitor);
            } else {
                ItemsList itemslist = expr.getRightItemsList();
                itemslist.accept(this);
            }
        }

        @Override
        protected void visitBinaryExpression(BinaryExpression expr) {
            Expression lhs = expr.getLeftExpression();
            Expression rhs = expr.getRightExpression();
            expressionReduce(expr, lhs, (expression, candidate) -> {
                expression.setLeftExpression(candidate);
                updateStatements();
            });
            expressionReduce(expr, rhs, (expression, candidate) -> {
                expression.setRightExpression(candidate);
                updateStatements();
            });
            lhs.accept(this);
            rhs.accept(this);
        }

        @Override
        public void visit(DateValue value) {
            super.visit(value);
        }

        @Override
        public void visit(TimeValue value) {
            super.visit(value);
        }

        @Override
        public void visit(LikeExpression expr) {
            super.visit(expr);
        }

        @Override
        public void visit(CaseExpression expr) {
            Expression switchExpr = expr.getSwitchExpression();
            Expression elseExpr = expr.getElseExpression();

            expressionReduce(expr, switchExpr, (parent, sw) -> {
                parent.setSwitchExpression(sw);
                updateStatements();
            });
            expressionReduce(expr, elseExpr, (parent, els) -> {
                parent.setElseExpression(els);
                updateStatements();
            });

            super.visit(expr);
        }

        @Override
        public void visit(WhenClause whenClause) {
            Expression when = whenClause.getWhenExpression();
            Expression then = whenClause.getThenExpression();

            expressionReduce(whenClause, when, (wc, w) -> {
                wc.setWhenExpression(w);
                updateStatements();
            });

            expressionReduce(whenClause, then, (wc, t) -> {
                wc.setThenExpression(t);
                updateStatements();
            });

            super.visit(whenClause);
        }

        @Override
        public void visit(Parenthesis parenthesis) {
            Expression closedExpr = parenthesis.getExpression();
            expressionReduce(parenthesis, closedExpr, (p, s) -> {
                p.setExpression(s);
                updateStatements();
            });
            closedExpr.accept(this);
        }

        @Override
        public void visit(Function function) {
            super.visit(function);
        }

        @Override
        public void visit(ExpressionList expressionList) {
            List<Expression> expressions = expressionList.getExpressions();
            listElementRemovingReduce(expressionList, expressions, (l, es) -> {
                l.setExpressions(es);
                updateStatements();
            });
            expressions = expressionList.getExpressions();
            for (int i = 0; i < expressions.size(); i++) {
                Expression expr = expressions.get(i);
                int index = i;
                expressionReduce(expressions, expr, (l, e) -> {
                    l.set(index, e);
                    updateStatements();
                });
            }
            super.visit(expressionList);
        }

        @Override
        public void visit(SelectExpressionItem selectExpressionItem) {
            Expression expr = selectExpressionItem.getExpression();
            expressionReduce(selectExpressionItem, expr, (item, e) -> {
                item.setExpression(e);
                updateStatements();
            });
            super.visit(selectExpressionItem);
        }
    };

    SelectVisitorAdapter selectReducerVisitor = new SelectVisitorAdapter() {
        private final String[] removeList = { "Limit", "Offset", "Where", "Having", "GroupBy", "Distinct",
                "OrderByElements", "Joins" };
        private final String[] pullUpList = { "Where", "Having", "FromItem", "SelectItems", "GroupBy", "Joins" };
        private final String[] descendList = { "Where", "Having", "FromItem", "SelectItems", "GroupBy" };

        private String getterName(String astNodeName) {
            return "get" + astNodeName;
        }

        private String setterName(String astNodeName) {
            if (astNodeName.equals("GroupBy")) {
                return "set" + astNodeName + "Element";
            } else {
                return "set" + astNodeName;
            }
        }

        @Override
        public void visit(PlainSelect plainSelect) {
            // Remove Section
            for (String astNodeName : removeList) {
                try {
                    Method nodeGetter = plainSelect.getClass().getMethod(getterName(astNodeName));
                    Object astNode = nodeGetter.invoke(plainSelect);
                    if (astNode == null) {
                        continue;
                    }
                    Method nodeSetter = plainSelect.getClass().getMethod(setterName(astNodeName),
                            nodeGetter.getReturnType());
                    nodeSetter.invoke(plainSelect, new Object[] { null });
                    updateStatements();
                    if (!bugStillTriggers()) {
                        nodeSetter.invoke(plainSelect, astNode);
                        updateStatements();
                    }
                } catch (Exception ignoredException) {
                }
            }

            // Pull Up Section
            for (String astNodeName : pullUpList) {
                try {
                    Method nodeGetter = plainSelect.getClass().getMethod(getterName(astNodeName));
                    Object astNode = nodeGetter.invoke(plainSelect);
                    if (astNode == null) {
                        continue;
                    }
                    Method nodeSetter = plainSelect.getClass().getMethod(setterName(astNodeName),
                            nodeGetter.getReturnType());

                    if (astNode instanceof Expression) {
                        Expression expr = (Expression) astNode;
                        expressionReduce(plainSelect, expr, (select, expression) -> {
                            try {
                                nodeSetter.invoke(select, expression);
                                updateStatements();
                            } catch (IllegalAccessException | InvocationTargetException e) {
                                e.printStackTrace();
                            }
                        });
                    } else if (astNode instanceof List) {
                        List<?> elms = (List<?>) astNode;
                        if (elms.size() <= 1) {
                            continue;
                        }
                        listElementRemovingReduce(plainSelect, elms, (select, items) -> {
                            try {
                                nodeSetter.invoke(select, items);
                                updateStatements();
                            } catch (IllegalAccessException | InvocationTargetException e) {
                                e.printStackTrace();
                            }
                        });
                    } else if (astNode instanceof GroupByElement) {
                        GroupByElement groupByElement = (GroupByElement) astNode;
                        ExpressionList expressionList = groupByElement.getGroupByExpressionList();
                        if (expressionList == null) {
                            groupByElement.getGroupingSets();
                            // TODO: TO BE IMPLEMENTED.
                        } else {
                            List<Expression> elms = expressionList.getExpressions();
                            if (elms.size() <= 1) {
                                continue;
                            }
                            listElementRemovingReduce(groupByElement, elms, (select, items) -> {
                                groupByElement.setGroupByExpressionList(new ExpressionList(items));
                                updateStatements();
                            });
                        }
                    }
                } catch (Exception ignoredException) {
                }
            }

            for (String astNodeName : descendList) {
                try {
                    Method nodeGetter = plainSelect.getClass().getMethod(getterName(astNodeName));
                    Object astNode = nodeGetter.invoke(plainSelect);
                    if (astNode == null) {
                        continue;
                    }
                    if (astNode instanceof Expression) {
                        ((Expression) astNode).accept(expressionReducerVisitor);
                    } else if (astNode instanceof List) {
                        // Really hacky... Some other ways to simplify it ?
                        List<?> elms = (List<?>) astNode;
                        for (Object obj : elms) {
                            if (obj instanceof SelectExpressionItem) {
                                ((SelectExpressionItem) obj).accept(expressionReducerVisitor);
                            } else if (obj instanceof Expression) {
                                ((Expression) obj).accept(expressionReducerVisitor);
                            }
                        }
                    } else if (astNode instanceof GroupByElement) {
                        GroupByElement groupByElement = (GroupByElement) astNode;
                        ExpressionList expressionList = groupByElement.getGroupByExpressionList();
                        if (expressionList != null) {
                            expressionList.accept(expressionReducerVisitor);
                        } else {
                            // TODO: TO BE IMPLEMENTED
                        }
                    }
                } catch (Exception e) {
                    System.out.println("Reflection Exception:");
                    e.printStackTrace();
                }
            }
        }

        @Override
        public void visit(SetOperationList setOpList) {
            List<SelectBody> selectBodies = setOpList.getSelects();
            listElementRemovingReduce(setOpList, selectBodies, (optionList, selects) -> {
                optionList.setSelects(selects);
                updateStatements();
            });
            for (SelectBody selectBody : selectBodies) {
                if (selectBody instanceof PlainSelect) {
                    visit((PlainSelect) selectBody);
                }
            }

        }
    };

    StatementVisitorAdapter statementReducerVisitor = new StatementVisitorAdapter() {
        @Override
        public void visit(Select select) {
            SelectBody selectBody = select.getSelectBody();
            if (selectBody != null) {
                selectBody.accept(selectReducerVisitor);
            }
        }
    };

    @Override
    public void reduce(G state, Reproducer<G> reproducer, G newGlobalState) throws Exception {
        this.state = state;
        this.newGlobalState = newGlobalState;
        this.reproducer = reproducer;

        List<Query<?>> initialBugInducingStatements = state.getState().getStatements();
        reducedStatements = new ArrayList<>();
        for (Query<?> query : initialBugInducingStatements) {
            reducedStatements.add((Query<C>) query);
        }

        for (int i = 0; i < reducedStatements.size(); i++) {
            reduceTargetIndex = i;
            Query<C> query = reducedStatements.get(reduceTargetIndex);
            targetStatement = CCJSqlParserUtil.parse(query.getQueryString());
            targetStatement.accept(statementReducerVisitor);
        }

        newGlobalState.getState().setStatements(new ArrayList<>(reducedStatements));
    }

    private boolean bugStillTriggers() throws Exception {
        try (C con2 = provider.createDatabase(newGlobalState)) {
            newGlobalState.setConnection(con2);
            List<Query<C>> candidateStatements = new ArrayList<>(reducedStatements);
            newGlobalState.getState().setStatements(new ArrayList<>(candidateStatements));

            for (Query<C> s : candidateStatements) {
                try {
                    s.execute(newGlobalState);
                } catch (Throwable ignoredException) {
                    // ignore
                }
            }
            try {
                if (reproducer.bugStillTriggers(newGlobalState)) {
                    return true;
                }
            } catch (Throwable ignoredException) {

            }
        }
        return false;
    }
}
