package sqlancer.oceanbase.oracle;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.oceanbase.OceanBaseErrors;
import sqlancer.oceanbase.OceanBaseGlobalState;
import sqlancer.oceanbase.OceanBaseSchema;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseTable;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseTables;
import sqlancer.oceanbase.ast.*;
import sqlancer.oceanbase.ast.OceanBaseUnaryPrefixOperation.OceanBaseUnaryPrefixOperator;
import sqlancer.oceanbase.gen.OceanBaseExpressionGenerator;
import sqlancer.oceanbase.gen.OceanBaseHintGenerator;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;

public abstract class OceanBaseTLPBase extends TernaryLogicPartitioningOracleBase<OceanBaseExpression, OceanBaseGlobalState> implements TestOracle {

    OceanBaseSchema s;
    OceanBaseTables targetTables;
    OceanBaseExpressionGenerator gen;
    OceanBaseSelect select;

    public OceanBaseTLPBase(OceanBaseGlobalState state) {
        super(state);
        OceanBaseErrors.addExpressionErrors(errors);
        errors.add("value is out of range");
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        gen = new OceanBaseExpressionGenerator(state).setColumns(targetTables.getColumns());
        initializeTernaryPredicateVariants();
        select = new OceanBaseSelect();
        select.setFetchColumns(generateFetchColumns());
        List<OceanBaseTable> tables = targetTables.getTables();
        OceanBaseHintGenerator.generateHints(select, tables);
        List<OceanBaseExpression> tableList = tables.stream().map(t -> new OceanBaseTableReference(t))
                .collect(Collectors.toList());
        select.setFromList(tableList);
        select.setWhereClause(null);
    }

    List<OceanBaseExpression> generateFetchColumns() {
        return Arrays.asList(OceanBaseColumnReference.create(targetTables.getColumns().get(0), null));
    }
    
    @Override
    protected ExpressionGenerator<OceanBaseExpression> getGen() {
        return gen;
    }

}
