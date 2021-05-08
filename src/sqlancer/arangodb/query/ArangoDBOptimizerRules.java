package sqlancer.arangodb.query;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;

public class ArangoDBOptimizerRules {

    private final List<String> allRules = new ArrayList<>();

    public ArangoDBOptimizerRules() {
        // SRC:
        // https://www.arangodb.com/docs/stable/aql/execution-and-performance-optimizer.html#list-of-optimizer-rules
        // Filtered out irrelevant ones
        allRules.add("-fuse-filters");
        // allRules.add("-geo-index-optimizer");
        // allRules.add("-handle-arangosearch-views");
        // allRules.add("-inline-subqueries");
        allRules.add("-interchange-adjacent-enumerations");
        allRules.add("-late-document-materialization");
        // allRules.add("-late-document-materialization-arangosearch");
        allRules.add("-move-calculations-down");
        allRules.add("-move-calculations-up");
        allRules.add("-move-filters-into-enumerate");
        allRules.add("-move-filters-up");
        // allRules.add("-optimize-count");
        // allRules.add("-optimize-subqueries");
        // allRules.add("-optimize-traversals");
        // allRules.add("-patch-update-statements");
        allRules.add("-propagate-constant-attributes");
        allRules.add("-reduce-extraction-to-projection");
        // allRules.add("-remove-collect-variables");
        // allRules.add("-remove-data-modification-out-variables");
        allRules.add("-remove-filter-covered-by-index");
        // allRules.add("-remove-filter-covered-by-traversal");
        allRules.add("-remove-redundant-calculations");
        allRules.add("-remove-redundant-or");
        // allRules.add("-remove-redundant-path-var");
        // allRules.add("-remove-redundant-sorts");
        // allRules.add("-remove-sort-rand");
        allRules.add("-remove-unnecessary-calculations");
        allRules.add("-remove-unnecessary-filters");
        // allRules.add("-replace-function-with-index");
        allRules.add("-replace-or-with-in");
        allRules.add("-simplify-conditions");
        // allRules.add("-sort-in-values");
        // allRules.add("-sort-limit");
        // allRules.add("-splice-subqueries");
        // allRules.add("-use-index-for-sort");
        allRules.add("-use-indexes");
    }

    public List<String> getRandomRules() {
        return Randomly.subset(allRules);
    }
}
