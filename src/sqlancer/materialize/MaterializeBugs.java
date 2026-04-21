package sqlancer.materialize;

// do not make the fields final to avoid warnings
public final class MaterializeBugs {

    // Tables or columns may be missing when reading information_schema shortly after creation
    public static boolean bugSchemaReadIncomplete = true;

    private MaterializeBugs() {
    }

}
