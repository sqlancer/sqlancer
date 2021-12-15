package sqlancer.oceanbase.gen;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.oceanbase.OceanBaseSchema;
import sqlancer.oceanbase.ast.OceanBaseConstant;
import sqlancer.oceanbase.ast.OceanBaseSelect;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseTable;
import sqlancer.oceanbase.ast.OceanBaseStringExpression;

import java.util.List;
import java.util.stream.Collectors;

public class OceanBaseHintGenerator {
    private OceanBaseSelect select;
    private List<OceanBaseTable> tables;
    private final StringBuilder sb = new StringBuilder();
    private Randomly r = new Randomly();

    enum IndexHint {
        MERGE_JOIN, 
        INL_JOIN, 
        INL_HASH_JOIN, 
        INL_MERGE_JOIN, 
        HASH_JOIN,
        HASH_AGG, 
        STREAM_AGG, 
        USE_INDEX, 
        IGNORE_INDEX, 
        AGG_TO_COP, 
        USE_INDEX_MERGE, 
        NO_INDEX_MERGE, 
        LEADING,
        PredDeduce,
        PDML,
        USE_TOJA;
    }

    public OceanBaseHintGenerator(OceanBaseSelect select, List<OceanBaseTable> tables) {
        this.select = select;
        this.tables = tables;
    }

    public static void generateHints(OceanBaseSelect select, List<OceanBaseTable> tables) {
        new OceanBaseHintGenerator(select, tables).generate();

    }

    private void generate() {
        OceanBaseTable table = Randomly.fromList(tables);
        switch (Randomly.fromOptions(IndexHint.values())) {
            case PDML:
                sb.append(" parallel(" + r.getInteger(0, 10) + "),enable_parallel_dml ");
                break;
            case PredDeduce:
                sb.append("no_pred_deduce");
                break;
            case MERGE_JOIN:
                tablesHint("USE_MERGE ");
                break;
            case INL_JOIN:
                tablesHint("USE_NL ");
                break;
            case LEADING:
                tablesHint(" LEADING ");
                break;
            case INL_HASH_JOIN:
                tablesHint("USE_HASH ");
                break;
            case INL_MERGE_JOIN:
                tablesHint("USE_BNL ");
                break;
            case HASH_JOIN:
                sb.append(" parallel(1) ");
                break;
            case HASH_AGG:
                sb.append("USE_HASH_AGGREGATION ");
                break;
            case STREAM_AGG:
                sb.append("USE_NL_MATERIALIZATION ");
                break;
            case USE_INDEX:
                indexesHint("INDEX_HINT ");
                break;
            case IGNORE_INDEX:
                sb.append("TOPK (50 50) ");
                break;
            case AGG_TO_COP:
                sb.append("USE_LATE_MATERIALIZATION ");
                break;
            case USE_INDEX_MERGE:
                sb.append("ORDERED ");
                break;
            case NO_INDEX_MERGE:
                tablesHint("NO_MERGE ");
                break;
            case USE_TOJA:
                sb.append("no_rewrite " );
                break;
            default:
                throw new AssertionError();
        }

        select.setHint(new OceanBaseStringExpression(sb.toString(),new OceanBaseConstant.OceanBaseTextConstant(sb.toString())));
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
