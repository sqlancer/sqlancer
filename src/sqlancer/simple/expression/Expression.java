package sqlancer.simple.expression;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import sqlancer.ReflectionException;
import sqlancer.simple.gen.Generator;

public interface Expression {
    String print();

    static Expression construct(Class<? extends Expression> clazz, Generator gen) {
        Class<?>[] parameterTypes = { Generator.class };
        try {
            Constructor<? extends Expression> constructor = clazz.getConstructor(parameterTypes);
            return constructor.newInstance(gen);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Constructor of " + clazz.getName() + "(" + Generator.class.getName()
                    + ")" + " not found for expression", e);
        } catch (InstantiationException e) {
            throw new ReflectionException("Unable to instantiate the expression", e);
        } catch (IllegalAccessException e) {
            throw new ReflectionException("Constructor is not accessible. Please update constructor's accessibility",
                    e);
        } catch (InvocationTargetException e) {
            throw new ReflectionException("Constructor threw an exception", e);
        }
    }
}
