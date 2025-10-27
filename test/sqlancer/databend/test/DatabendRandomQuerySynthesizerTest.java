package sqlancer.databend.test;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import sqlancer.databend.DatabendProvider.DatabendGlobalState;
import sqlancer.databend.ast.DatabendSelect;
import sqlancer.databend.gen.DatabendRandomQuerySynthesizer;

public class DatabendRandomQuerySynthesizerTest {

    @Test
    public void testDistinctOrderByReferencesSelectList() {
        DatabendGlobalState state = new DatabendGlobalState();
        DatabendSelect select = DatabendRandomQuerySynthesizer.generateSelect(state, 3);
        
        // Set distinct to true to test our new behavior
        select.setDistinct(true);
        
        // If there are ORDER BY clauses in a DISTINCT query
        if (select.getOrderByClauses() != null && !select.getOrderByClauses().isEmpty()) {
            // Each ORDER BY expression should be a number referencing a position in the select list
            select.getOrderByClauses().forEach(expr -> {
                assertTrue(expr.toString().matches("\\d+"), 
                    "ORDER BY expression should be a number in DISTINCT queries");
                int position = Integer.parseInt(expr.toString());
                assertTrue(position > 0 && position <= select.getFetchColumns().size(),
                    "ORDER BY position should reference a valid select list item");
            });
        }
    }
} 