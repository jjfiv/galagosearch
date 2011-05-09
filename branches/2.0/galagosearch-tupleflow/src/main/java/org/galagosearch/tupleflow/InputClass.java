// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.tupleflow;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 *
 * @author trevor
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface InputClass {
    String className() default "java.lang.Object";

    String[] order() default {};
}
