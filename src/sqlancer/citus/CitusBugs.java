package sqlancer.citus;

// do not make the fields final to avoid warnings
public final class CitusBugs {

    // https://github.com/citusdata/citus/issues/3987
    public static boolean bug3987;

    // https://github.com/citusdata/citus/issues/3980
    public static boolean bug3980;

    // https://github.com/citusdata/citus/issues/3957
    public static boolean bug3957;

    // https://github.com/citusdata/citus/issues/4019
    public static boolean bug4019 = true;

    // https://github.com/citusdata/citus/issues/4013
    public static boolean bug4013 = true;

    // https://github.com/citusdata/citus/issues/3982
    public static boolean bug3982 = true;

    // https://github.com/citusdata/citus/issues/3981
    public static boolean bug3981 = true;

    // https://github.com/citusdata/citus/issues/4014
    public static boolean bug4014 = true;

    // https://github.com/citusdata/citus/issues/4079
    public static boolean bug4079 = true;

    private CitusBugs() {
    }

}
