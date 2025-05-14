package sqlancer.oxla;

public final class OxlaBugs {
    private OxlaBugs() {
    }

    // https://oxla.atlassian.net/browse/OXLA-3376
    // Valid INT_MIN value results in an "Integer literal error. Value of literal exceeds range." parsing error.
    public static boolean bugOxla3376 = true;

    // https://oxla.atlassian.net/browse/OXLA-8323
    // Errors caused in JOIN's WHERE condition return internal error non-deterministically.
    public static boolean bugOxla8323 = true;
}
