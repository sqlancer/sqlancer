package sqlancer.oceanbase.gen;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.oceanbase.OceanBaseSchema;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseTable;
import sqlancer.oceanbase.ast.OceanBaseConstant;
import sqlancer.oceanbase.ast.OceanBaseSelect;
import sqlancer.oceanbase.ast.OceanBaseStringExpression;

public class OceanBaseHintGenerator {
    private final OceanBaseSelect select;
    private final List<OceanBaseTable> tables;
    private final StringBuilder sb = new StringBuilder();
    private final Randomly r = new Randomly();

    enum IndexHint {
        PDML, NO_PRED_DEDUCE, MERGE_JOIN, HASH_JOIN, NL_JOIN, BNL_JOIN, NO_MERGE_JOIN, NO_HASH_JOIN, NO_NL_JOIN,
        NO_BNL_JOIN, HASH_AGG, NL_MATERIALIZATION, LATE_MATERIALIZATION, USE_INDEX, TOPK, LEADING, ORDERED, NO_REWRITE;
    }

    public OceanBaseHintGenerator(OceanBaseSelect select, List<OceanBaseTable> tables) {
        this.select = select;
        this.tables = tables;
    }

    public static void generateHints(OceanBaseSelect select, List<OceanBaseTable> tables) {
        new OceanBaseHintGenerator(select, tables).generate();

    }

    private void generate() {
        switch (Randomly.fromOptions(IndexHint.values())) {
        case PDML:
            sb.append(" parallel(" + r.getInteger(0, 10) + "),enable_parallel_dml ");
            break;
        case NO_PRED_DEDUCE:
            sb.append("NO_PRED_DEDUCE");
            break;
        case MERGE_JOIN:
            tablesHint("USE_MERGE ");
            break;
        case HASH_JOIN:
            tablesHint("USE_HASH ");
            break;
        case NL_JOIN:
            tablesHint("USE_NL ");
            break;
        case BNL_JOIN:
            tablesHint("USE_BNL ");
            break;
        case NO_MERGE_JOIN:
            sb.append(" NO_USE_MERGE ");
            break;
        case NO_HASH_JOIN:
            sb.append(" NO_USE_HASH ");
            break;
        case NO_NL_JOIN:
            sb.append(" NO_USE_NL ");
            break;
        case NO_BNL_JOIN:
            sb.append(" NO_USE_BNL ");
            break;
        case HASH_AGG:
            sb.append("USE_HASH_AGGREGATION ");
            break;
        case NL_MATERIALIZATION:
            sb.append("USE_NL_MATERIALIZATION ");
            break;
        case LATE_MATERIALIZATION:
            sb.append("USE_LATE_MATERIALIZATION ");
            break;
        case USE_INDEX:
            indexesHint("INDEX_HINT ");
            break;
        case TOPK:
            sb.append("TOPK (50 50) ");
            break;
        case LEADING:
            tablesHint(" LEADING ");
            break;
        case ORDERED:
            sb.append("ORDERED ");
            break;
        case NO_REWRITE:
            sb.append("NO_REWRITE ");
            break;
        default:
            throw new AssertionError();
        }

        select.setHint(new OceanBaseStringExpression(sb.toString(),
                new OceanBaseConstant.OceanBaseTextConstant(sb.toString())));
    }

    private void indexesHint(String string) {
        sb.append(string);
        sb.append("(");
        OceanBaseTable table = Randomly.fromList(tables);
        List<OceanBaseSchema.OceanBaseIndex> allIndexes = table.getIndexes();
        if (allIndexes.isEmpty()) {
            throw new IgnoreMeException();
        }
        List<OceanBaseSchema.OceanBaseIndex> indexSubset = Randomly.nonEmptySubset(allIndexes);
        sb.append(table.getName());
        sb.append(", ");
        sb.append(indexSubset.stream().map(i -> i.getIndexName()).distinct().collect(Collectors.joining(", ")));
        sb.append(")");
    }

    private void tablesHint(String string) {
        sb.append(string);
        sb.append("(");
        appendTables();
        sb.append(")");
    }

    private void appendTables() {
        List<OceanBaseTable> tableSubset = Randomly.nonEmptySubset(tables);
        sb.append(tableSubset.stream().map(t -> t.getName()).collect(Collectors.joining(", ")));
    }
}
