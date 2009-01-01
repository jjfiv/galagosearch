// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.tupleflow.typebuilder;

/**
 *
 * @author trevor
 */
public class FieldSpecification {
    public enum DataType {
        BOOLEAN ("boolean", "boolean", "Boolean", false, false, false),
        BYTE   ("byte", "byte", "Byte", true, false, false),
        SHORT  ("short", "short", "Short", true, false, false),
        INT    ("int", "int", "Integer", true, false, false),
        LONG   ("long", "long", "Long", true, false, false),
        FLOAT  ("float", "float", "Float", false, false, false),
        DOUBLE ("double", "double", "Double", false, false, false),
        STRING ("String", "String", "String", false, true, false),
        BYTES  ("bytes", "byte", "byte[]", false, false, true);
                
        DataType(String internalType, String baseType, String className,
                 boolean isInteger, boolean isString, boolean isArray) {
            this.internalType = internalType;
            this.baseType = baseType;
            this.className = className;
            this.isInteger = isInteger;
            this.isString = isString;
            this.isArray = isArray;
        }

        public String getType() {
            if (!isArray)
                return baseType;
            return baseType + "[]";
        }
        
        public String getBaseType() {
            return baseType;
        }
        
        public String getInternalType() {
            return internalType;
        }

        public boolean isInteger() {
            return isInteger;
        }

        public boolean isString() {
            return isString;
        }
        
        public boolean isArray() {
            return isArray;
        }

        public String getClassName() {
            return className;
        }
        
        private String baseType;
        private String internalType;
        private String className;
        private boolean isInteger;
        private boolean isString;
        private boolean isArray;
    };
    
    public FieldSpecification(DataType type, String name) {
        this.type = type;
        this.name = name;
    }
    
    public DataType getType() {
        return type;
    }
    
    public String getName() {
        return name;
    }
    
    protected DataType type;
    protected String name;
}
