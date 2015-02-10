/**
 * Copyright (C) 2014 Dasasian (damith@dasasian.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dasasian.chok.util;

import org.apache.hadoop.io.*;

/**
 * Helper class for dealing with hadoop writable <-> java primitive wrapper
 * conversion.
 *
 * @see Writable
 */
public enum WritableType {

    TEXT, BYTE, INT, LONG, FLOAT, DOUBLE;

    public static WritableType detectWritableType(Object comparable) {
        if (comparable instanceof Byte) {
            return WritableType.BYTE;
        } else if (comparable instanceof Integer) {
            return WritableType.INT;
        } else if (comparable instanceof String) {
            return WritableType.TEXT;
        } else if (comparable instanceof Float) {
            return WritableType.FLOAT;
        } else if (comparable instanceof Long) {
            return WritableType.LONG;
        } else if (comparable instanceof Double) {
            return WritableType.DOUBLE;
        }
        throw new IllegalArgumentException("no conversion rule for comparable of type " + comparable.getClass().getName());
    }

    public static WritableType[] detectWritableTypes(Object[] comparables) {
        WritableType[] writablTypes = new WritableType[comparables.length];
        for (int i = 0; i < comparables.length; i++) {
            writablTypes[i] = detectWritableType(comparables[i]);
        }
        return writablTypes;
    }

    public static WritableComparable[] convertComparable(WritableType[] writableTypes, Object[] comparables) {
        WritableComparable[] writableComparables = new WritableComparable[comparables.length];
        for (int i = 0; i < writableComparables.length; i++) {
            writableComparables[i] = writableTypes[i].convertComparable(comparables[i]);

        }
        return writableComparables;
    }

    public WritableComparable newWritableComparable() {
        switch (this) {
            case TEXT:
                return new Text();
            case BYTE:
                return new ByteWritable();
            case INT:
                return new IntWritable();
            case LONG:
                return new LongWritable();
            case FLOAT:
                return new FloatWritable();
            case DOUBLE:
                return new DoubleWritable();
        }
        throw getUnhandledTypeException();
    }

    /**
     * Convert a java primitive type wrapper (like String, Integer, Float, etc...)
     * to the corresponding hadoop {@link WritableComparable}.
     */
    public WritableComparable convertComparable(Object object) {
        switch (this) {
            case TEXT:
                return new Text((String) object);
            case BYTE:
                return new ByteWritable((Byte) object);
            case INT:
                return new IntWritable((Integer) object);
            case LONG:
                return new LongWritable(((Long) object));
            case FLOAT:
                return new FloatWritable((Float) object);
            case DOUBLE:
                return new DoubleWritable((Double) object);
        }
        throw getUnhandledTypeException();
    }

    private RuntimeException getUnhandledTypeException() {
        return new IllegalArgumentException("type " + this + " not handled");
    }

}
