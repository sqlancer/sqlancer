package sqlancer.clickhouse.ast;

import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.stream.Collectors;

import com.clickhouse.client.ClickHouseDataType;
import sqlancer.clickhouse.ast.constant.ClickHouseCreateConstant;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ClickHouseBinaryComparisonOperationTest {

    @Test
    void getExpectedValueTrueEqualsTrue() {
        ClickHouseConstant trueConst = ClickHouseCreateConstant.createTrue();
        ClickHouseConstant equals = trueConst.applyEquals(ClickHouseCreateConstant.createTrue());
        assertEquals(true, equals.asBooleanNotNull());
    }

    @Test
    void getExpectedValueTrueNotEqualsFalse() {
        ClickHouseConstant trueConst = ClickHouseCreateConstant.createTrue();
        ClickHouseConstant falseConst = ClickHouseCreateConstant.createFalse();
        ClickHouseConstant equals = trueConst.applyEquals(ClickHouseCreateConstant.createFalse());
        ClickHouseConstant equalsFalse = falseConst.applyEquals(ClickHouseCreateConstant.createTrue());
        assertEquals(false, equals.asBooleanNotNull());
        assertEquals(false, equalsFalse.asBooleanNotNull());
    }

    @Test
    void getExpectedValueFloat64EqualsFloat64() {
        ClickHouseConstant oneConst = ClickHouseCreateConstant.createFloat64Constant(1);
        ClickHouseConstant oneFConst = ClickHouseCreateConstant.createFloat64Constant(1.0);
        ClickHouseConstant zeroConst = ClickHouseCreateConstant.createFloat64Constant(0);
        ClickHouseConstant zeroFConst = ClickHouseCreateConstant.createFloat64Constant(0.0);

        assertEquals(true, oneConst.applyEquals(oneConst).asBooleanNotNull());
        assertEquals(true, oneFConst.applyEquals(oneFConst).asBooleanNotNull());
        assertEquals(true, oneConst.applyEquals(oneFConst).asBooleanNotNull());

        assertEquals(false, oneConst.applyEquals(zeroConst).asBooleanNotNull());
        assertEquals(false, oneFConst.applyEquals(zeroFConst).asBooleanNotNull());
        assertEquals(true, zeroConst.applyEquals(zeroFConst).asBooleanNotNull());
        assertEquals(true, zeroFConst.applyEquals(zeroConst).asBooleanNotNull());
    }

    @Test
    void getExpectedValueInt32EqualsBool() {
        ClickHouseConstant trueConst = ClickHouseCreateConstant.createTrue();
        ClickHouseConstant falseConst = ClickHouseCreateConstant.createFalse();
        ClickHouseConstant oneConst = ClickHouseCreateConstant.createInt32Constant(1);
        ClickHouseConstant zeroConst = ClickHouseCreateConstant.createInt32Constant(0);
        ClickHouseConstant negativeConst = ClickHouseCreateConstant.createInt32Constant(-100);
        ClickHouseConstant positiveConst = ClickHouseCreateConstant.createInt32Constant(10000);

        assertEquals(true, trueConst.applyEquals(oneConst).asBooleanNotNull());
        assertEquals(true, oneConst.applyEquals(oneConst).asBooleanNotNull());
        assertEquals(false, falseConst.applyEquals(oneConst).asBooleanNotNull());

        assertEquals(false, trueConst.applyEquals(zeroConst).asBooleanNotNull());
        assertEquals(false, oneConst.applyEquals(zeroConst).asBooleanNotNull());
        assertEquals(true, falseConst.applyEquals(zeroConst).asBooleanNotNull());

        assertEquals(false, negativeConst.applyEquals(oneConst).asBooleanNotNull());
        assertEquals(false, negativeConst.applyEquals(zeroConst).asBooleanNotNull());
        assertEquals(false, negativeConst.applyEquals(trueConst).asBooleanNotNull());
        assertEquals(false, negativeConst.applyEquals(falseConst).asBooleanNotNull());

        assertEquals(false, positiveConst.applyEquals(oneConst).asBooleanNotNull());
        assertEquals(false, positiveConst.applyEquals(zeroConst).asBooleanNotNull());
        assertEquals(false, positiveConst.applyEquals(trueConst).asBooleanNotNull());
        assertEquals(false, positiveConst.applyEquals(falseConst).asBooleanNotNull());
    }

    @Test
    void getExpectedValueIntEqualsInt() {
        ClickHouseConstant trueConst = ClickHouseCreateConstant.createTrue();
        ClickHouseConstant falseConst = ClickHouseCreateConstant.createFalse();
        for (ClickHouseDataType type : Arrays.<ClickHouseDataType> stream(ClickHouseDataType.values())
                .filter((dt) -> dt.name().contains("Int") && !dt.name().contains("Interval"))
                .collect(Collectors.toList())) {
            ClickHouseConstant oneConst = ClickHouseCreateConstant.createIntConstant(type, 1);
            ClickHouseConstant zeroConst = ClickHouseCreateConstant.createIntConstant(type, 0);
            ClickHouseConstant negativeConst = ClickHouseCreateConstant.createIntConstant(type, -100);
            ClickHouseConstant positiveConst = ClickHouseCreateConstant.createIntConstant(type, 10000);

            assertEquals(true, trueConst.applyEquals(oneConst).asBooleanNotNull());
            assertEquals(true, oneConst.applyEquals(oneConst).asBooleanNotNull());
            assertEquals(false, falseConst.applyEquals(oneConst).asBooleanNotNull());

            assertEquals(false, trueConst.applyEquals(zeroConst).asBooleanNotNull());
            assertEquals(false, oneConst.applyEquals(zeroConst).asBooleanNotNull());
            assertEquals(true, falseConst.applyEquals(zeroConst).asBooleanNotNull());

            assertEquals(false, negativeConst.applyEquals(oneConst).asBooleanNotNull());
            assertEquals(false, negativeConst.applyEquals(zeroConst).asBooleanNotNull());
            assertEquals(false, negativeConst.applyEquals(trueConst).asBooleanNotNull());
            assertEquals(false, negativeConst.applyEquals(falseConst).asBooleanNotNull());

            assertEquals(false, positiveConst.applyEquals(oneConst).asBooleanNotNull());
            assertEquals(false, positiveConst.applyEquals(zeroConst).asBooleanNotNull());
            assertEquals(false, positiveConst.applyEquals(trueConst).asBooleanNotNull());
            assertEquals(false, positiveConst.applyEquals(falseConst).asBooleanNotNull());
        }
    }

    @Test
    void getExpectedValueInt32EqualsFloat64() {
        ClickHouseConstant float64OneConst = ClickHouseCreateConstant.createFloat64Constant(1.0);
        ClickHouseConstant float64ZeroConst = ClickHouseCreateConstant.createFloat64Constant(0.0);
        ClickHouseConstant oneConst = ClickHouseCreateConstant.createInt32Constant(1);
        ClickHouseConstant zeroConst = ClickHouseCreateConstant.createInt32Constant(0);
        ClickHouseConstant negativeConst = ClickHouseCreateConstant.createInt32Constant(-100);
        ClickHouseConstant positiveConst = ClickHouseCreateConstant.createInt32Constant(10000);

        assertEquals(true, float64OneConst.applyEquals(oneConst).asBooleanNotNull());
        assertEquals(false, float64ZeroConst.applyEquals(oneConst).asBooleanNotNull());

        assertEquals(false, float64OneConst.applyEquals(zeroConst).asBooleanNotNull());
        assertEquals(true, float64ZeroConst.applyEquals(zeroConst).asBooleanNotNull());

        assertEquals(false, negativeConst.applyEquals(float64OneConst).asBooleanNotNull());
        assertEquals(false, negativeConst.applyEquals(float64ZeroConst).asBooleanNotNull());

        assertEquals(false, positiveConst.applyEquals(float64OneConst).asBooleanNotNull());
        assertEquals(false, positiveConst.applyEquals(float64ZeroConst).asBooleanNotNull());
    }

    @Test
    void getExpectedValueInt64EqualsFloat64() {

        ClickHouseConstant bigInt = ClickHouseCreateConstant.createInt64Constant(new BigInteger("9223372036854775807"));
        ClickHouseConstant floatVal = ClickHouseCreateConstant.createFloat64Constant(9223372036854775807.0);

        assertEquals(true, bigInt.applyEquals(floatVal).asBooleanNotNull());
    }

    @Test
    void getExpectedValueNullEqualsNull() {
        ClickHouseConstant nullConst = ClickHouseCreateConstant.createNullConstant();
        ClickHouseConstant equals = nullConst.applyEquals(ClickHouseCreateConstant.createNullConstant());
        assertEquals(true, equals.isNull());
    }

    // how ClickHouse deals with edge cases involving large numbers and floating-point precision.
    @Test
    void testInt64Float64PrecisionLoss() {
        // Int64 max value (9,223,372,036,854,775,807) cannot be exactly represented as Float64
        ClickHouseConstant bigInt = ClickHouseCreateConstant.createInt64Constant(new BigInteger("9223372036854775807"));
        ClickHouseConstant approxFloat = ClickHouseCreateConstant.createFloat64Constant(9223372036854776808.0);

        // ClickHouse rounds both to the nearest representable Float64 value (9,223,372,036,854,776,000) not (9,223,372,036,854,776,808.0)
        assertEquals(true, bigInt.applyEquals(approxFloat).asBooleanNotNull());
    }
}
