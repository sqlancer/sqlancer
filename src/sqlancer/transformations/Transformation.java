package sqlancer.transformations;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import net.sf.jsqlparser.statement.Statement;

/**
 * The base class of transformations Defines APIs to remove, replace, remove elements of a list.
 */
public class Transformation {

    private static Supplier<Boolean> bugJudgement;
    protected boolean isChanged;
    String current;
    Statement statement;
    private String desc = "";

    public Transformation(String desc) {
        this.desc = desc;
    }

    @SuppressWarnings("unused")
    protected Transformation() {
    }

    public static void setBugJudgement(Supplier<Boolean> judgement) {
        bugJudgement = judgement;
    }

    @Override
    public String toString() {
        return desc;
    }

    public boolean init(String sql) {
        isChanged = false;
        return true;
    }

    public <P, T> boolean tryRemove(P parent, T target, BiConsumer<P, T> setter) {
        setter.accept(parent, null);
        onStatementChanged();
        if (!bugStillTriggers()) {
            setter.accept(parent, target);
            onStatementChanged();
            return false;
        }
        isChanged = true;
        return true;
    }

    public <P, T> boolean tryReplace(P parent, T original, T vari, BiConsumer<P, T> setter) {
        setter.accept(parent, vari);
        onStatementChanged();
        if (!bugStillTriggers()) {
            setter.accept(parent, original);
            onStatementChanged();
            return false;
        }
        isChanged = true;
        return true;
    }

    public <P, T> void tryRemoveElms(P parent, List<T> elms, // NOPMD
            BiConsumer<P, List<T>> setter) {
        boolean observeChange;
        do {
            observeChange = false;
            for (int i = elms.size() - 1; i >= 0; i--) {
                List<T> reducedElms = new ArrayList<>(elms);
                reducedElms.subList(i, i + 1).clear();
                setter.accept(parent, reducedElms);
                onStatementChanged();
                if (bugStillTriggers()) {
                    elms = reducedElms;
                    onStatementChanged();
                    observeChange = true;
                }
            }
            isChanged |= observeChange;
            setter.accept(parent, elms);
            onStatementChanged();
        } while (observeChange);

    }

    public boolean bugStillTriggers() {
        try {
            return Transformation.bugJudgement.get();
        } catch (Exception ignored) {
        }
        return false;
    }

    public void apply() {
        isChanged = false;
    }

    public String getResult() {
        return statement.toString();
    }

    public boolean changed() {
        return isChanged;
    }

    protected void onStatementChanged() {
    }
}
