package sqlancer.simple.clause;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import sqlancer.ReflectionException;
import sqlancer.simple.gen.Generator;

public interface Clause {
    String print();

    static Clause construct(Class<? extends Clause> clazz, Generator gen) {
        Class<?>[] parameterTypes = { Generator.class };
        try {
            Constructor<? extends Clause> constructor = clazz.getConstructor(parameterTypes);
            return constructor.newInstance(gen);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Constructor of " + clazz.getName() + "(" + Generator.class.getName()
                    + ")" + " not found for " + clazz.getName(), e);
        } catch (InstantiationException e) {
            throw new ReflectionException("Unable to instantiate the " + clazz.getName(), e);
        } catch (IllegalAccessException e) {
            throw new ReflectionException("Constructor is not accessible. Please update constructor's accessibility",
                    e);
        } catch (InvocationTargetException e) {
            throw new ReflectionException("Constructor for " + clazz.getName() + " threw an exception", e);
        }
    }

}
