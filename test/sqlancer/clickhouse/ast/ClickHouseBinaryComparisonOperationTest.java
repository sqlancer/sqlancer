package sqlancer.clickhouse.ast;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.stream.Collectors;

import ru.yandex.clickhouse.domain.ClickHouseDataType;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ClickHouseBinaryComparisonOperationTest {

    @Test
    void getExpectedValueTrueEqualsTrue() {
        ClickHouseConstant trueConst = ClickHouseConstant.createTrue();
        ClickHouseConstant equals = trueConst.applyEquals(ClickHouseConstant.createTrue());
        assertEquals(equals.asInt(), 1);
    }

    @Test
    void getExpectedValueTrueNotEqualsFalse() {
        ClickHouseConstant trueConst = ClickHouseConstant.createTrue();
        ClickHouseConstant falseConst = ClickHouseConstant.createFalse();
        ClickHouseConstant equals = trueConst.applyEquals(ClickHouseConstant.createFalse());
        ClickHouseConstant equalsFalse = falseConst.applyEquals(ClickHouseConstant.createTrue());
        assertEquals(equals.asInt(), 0);
        assertEquals(equalsFalse.asInt(), 0);
    }

    @Test
    void getExpectedValueFloat64EqualsFloat64() {
        ClickHouseConstant oneConst = ClickHouseConstant.createFloat64Constant(1);
        ClickHouseConstant oneFConst = ClickHouseConstant.createFloat64Constant(1.0);
        ClickHouseConstant zeroConst = ClickHouseConstant.createFloat64Constant(0);
        ClickHouseConstant zeroFConst = ClickHouseConstant.createFloat64Constant(0.0);

        assertEquals(oneConst.applyEquals(oneConst).asInt(), 1);
        assertEquals(oneFConst.applyEquals(oneFConst).asInt(), 1);
        assertEquals(oneConst.applyEquals(oneFConst).asInt(), 1);

        assertEquals(oneConst.applyEquals(zeroConst).asInt(), 0);
        assertEquals(oneFConst.applyEquals(zeroFConst).asInt(), 0);
        assertEquals(zeroConst.applyEquals(zeroFConst).asInt(), 1);
        assertEquals(zeroFConst.applyEquals(zeroConst).asInt(), 1);
    }

    @Test
    void getExpectedValueInt32EqualsBool() {
        ClickHouseConstant trueConst = ClickHouseConstant.createTrue();
        ClickHouseConstant falseConst = ClickHouseConstant.createFalse();
        ClickHouseConstant oneConst = ClickHouseConstant.createInt32Constant(1);
        ClickHouseConstant zeroConst = ClickHouseConstant.createInt32Constant(0);
        ClickHouseConstant negativeConst = ClickHouseConstant.createInt32Constant(-100);
        ClickHouseConstant positiveConst = ClickHouseConstant.createInt32Constant(10000);

        assertEquals(trueConst.applyEquals(oneConst).asInt(), 1);
        assertEquals(oneConst.applyEquals(oneConst).asInt(), 1);
        assertEquals(falseConst.applyEquals(oneConst).asInt(), 0);

        assertEquals(trueConst.applyEquals(zeroConst).asInt(), 0);
        assertEquals(oneConst.applyEquals(zeroConst).asInt(), 0);
        assertEquals(falseConst.applyEquals(zeroConst).asInt(), 1);

        assertEquals(negativeConst.applyEquals(oneConst).asInt(), 0);
        assertEquals(negativeConst.applyEquals(zeroConst).asInt(), 0);
        assertEquals(negativeConst.applyEquals(trueConst).asInt(), 0);
        assertEquals(negativeConst.applyEquals(falseConst).asInt(), 0);

        assertEquals(positiveConst.applyEquals(oneConst).asInt(), 0);
        assertEquals(positiveConst.applyEquals(zeroConst).asInt(), 0);
        assertEquals(positiveConst.applyEquals(trueConst).asInt(), 0);
        assertEquals(positiveConst.applyEquals(falseConst).asInt(), 0);
    }

    @Test
    void getExpectedValueIntEqualsInt() {
        ClickHouseConstant trueConst = ClickHouseConstant.createTrue();
        ClickHouseConstant falseConst = ClickHouseConstant.createFalse();
        for (ClickHouseDataType type : Arrays.<ClickHouseDataType> stream(ClickHouseDataType.values())
                .filter((dt) -> dt.name().contains("Int") && !dt.name().contains("Interval"))
                .collect(Collectors.toList())) {
            ClickHouseConstant oneConst = ClickHouseConstant.createIntConstant(type, 1);
            ClickHouseConstant zeroConst = ClickHouseConstant.createIntConstant(type, 0);
            ClickHouseConstant negativeConst = ClickHouseConstant.createIntConstant(type, -100);
            ClickHouseConstant positiveConst = ClickHouseConstant.createIntConstant(type, 10000);

            assertEquals(trueConst.applyEquals(oneConst).asInt(), 1);
            assertEquals(oneConst.applyEquals(oneConst).asInt(), 1);
            assertEquals(falseConst.applyEquals(oneConst).asInt(), 0);

            assertEquals(trueConst.applyEquals(zeroConst).asInt(), 0);
            assertEquals(oneConst.applyEquals(zeroConst).asInt(), 0);
            assertEquals(falseConst.applyEquals(zeroConst).asInt(), 1);

            assertEquals(negativeConst.applyEquals(oneConst).asInt(), 0);
            assertEquals(negativeConst.applyEquals(zeroConst).asInt(), 0);
            assertEquals(negativeConst.applyEquals(trueConst).asInt(), 0);
            assertEquals(negativeConst.applyEquals(falseConst).asInt(), 0);

            assertEquals(positiveConst.applyEquals(oneConst).asInt(), 0);
            assertEquals(positiveConst.applyEquals(zeroConst).asInt(), 0);
            assertEquals(positiveConst.applyEquals(trueConst).asInt(), 0);
            assertEquals(positiveConst.applyEquals(falseConst).asInt(), 0);
        }
    }

    @Test
    void getExpectedValueInt32EqualsFloat64() {
        ClickHouseConstant float64OneConst = ClickHouseConstant.createFloat64Constant(1.0);
        ClickHouseConstant float64ZeroConst = ClickHouseConstant.createFloat64Constant(0.0);
        ClickHouseConstant oneConst = ClickHouseConstant.createInt32Constant(1);
        ClickHouseConstant zeroConst = ClickHouseConstant.createInt32Constant(0);
        ClickHouseConstant negativeConst = ClickHouseConstant.createInt32Constant(-100);
        ClickHouseConstant positiveConst = ClickHouseConstant.createInt32Constant(10000);

        assertEquals(float64OneConst.applyEquals(oneConst).asInt(), 1);
        assertEquals(float64ZeroConst.applyEquals(oneConst).asInt(), 0);

        assertEquals(float64OneConst.applyEquals(zeroConst).asInt(), 0);
        assertEquals(float64ZeroConst.applyEquals(zeroConst).asInt(), 1);

        assertEquals(negativeConst.applyEquals(float64OneConst).asInt(), 0);
        assertEquals(negativeConst.applyEquals(float64ZeroConst).asInt(), 0);

        assertEquals(positiveConst.applyEquals(float64OneConst).asInt(), 0);
        assertEquals(positiveConst.applyEquals(float64ZeroConst).asInt(), 0);
    }
}
