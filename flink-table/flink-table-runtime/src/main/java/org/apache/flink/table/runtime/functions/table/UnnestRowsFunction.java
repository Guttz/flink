/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.functions.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.runtime.functions.BuiltInSpecializedFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;

/**
 * Flattens ARRAY, MAP, and MULTISET using a table function. It does this by another level of
 * specialization using a subclass of {@link UnnestTableFunctionBase}.
 */
@Internal
public class UnnestRowsFunction extends BuiltInSpecializedFunction {

    public UnnestRowsFunction() {
        super(BuiltInFunctionDefinitions.INTERNAL_UNNEST_ROWS);
    }

    @Override
    public UserDefinedFunction specialize(SpecializedContext context) {
        final LogicalType argType =
                context.getCallContext().getArgumentDataTypes().get(0).getLogicalType();
        //final boolean withOrdinality = context.getCallContext().isWithOrdinality();
        final boolean withOrdinality = true;
        switch (argType.getTypeRoot()) {
            case ARRAY:
                final ArrayType arrayType = (ArrayType) argType;
                return new CollectionUnnestTableFunction(
                        context,
                        arrayType.getElementType(),
                        ArrayData.createElementGetter(arrayType.getElementType()),
                        withOrdinality);
            case MULTISET:
                final MultisetType multisetType = (MultisetType) argType;
                return new CollectionUnnestTableFunction(
                        context,
                        multisetType.getElementType(),
                        ArrayData.createElementGetter(multisetType.getElementType()),
                        withOrdinality);
            case MAP:
                final MapType mapType = (MapType) argType;
                return new MapUnnestTableFunction(
                        context,
                        RowType.of(false, mapType.getKeyType(), mapType.getValueType()),
                        ArrayData.createElementGetter(mapType.getKeyType()),
                        ArrayData.createElementGetter(mapType.getValueType()),
                        withOrdinality);
            default:
                throw new UnsupportedOperationException("Unsupported type for UNNEST: " + argType);
        }
    }

    public static LogicalType getUnnestedType(LogicalType logicalType, boolean withOrdinality) {
        LogicalType baseType;
        switch (logicalType.getTypeRoot()) {
            case ARRAY:
                baseType = ((ArrayType) logicalType).getElementType();
                break;
            case MULTISET:
                baseType = ((MultisetType) logicalType).getElementType();
                break;
            case MAP:
                final MapType mapType = (MapType) logicalType;
                baseType = RowType.of(false, mapType.getKeyType(), mapType.getValueType());
                break;
            default:
                throw new UnsupportedOperationException("Unsupported UNNEST type: " + logicalType);
        }

        if (withOrdinality) {
            if (baseType instanceof RowType) {
                // For row types, add the ordinal field
                RowType rowType = (RowType) baseType;
                LogicalType[] fieldTypes = new LogicalType[rowType.getFieldCount() + 1];
                String[] fieldNames = new String[rowType.getFieldCount() + 1];
                for (int i = 0; i < rowType.getFieldCount(); i++) {
                    fieldTypes[i] = rowType.getTypeAt(i);
                    fieldNames[i] = rowType.getFieldNames().get(i);
                }
                fieldTypes[rowType.getFieldCount()] = DataTypes.INT().notNull().getLogicalType();
                fieldNames[rowType.getFieldCount()] = "ordinality";
                return RowType.of(false, fieldTypes, fieldNames);
            } else {
                // For non-row types, wrap in a row with the original type and ordinal field
                return RowType.of(
                    false,
                    new LogicalType[]{baseType, DataTypes.INT().notNull().getLogicalType()},
                    new String[]{"f0", "ordinality"});
            }
        }
        return baseType;
    }

    // --------------------------------------------------------------------------------------------
    // Runtime Implementation
    // --------------------------------------------------------------------------------------------

    private abstract static class UnnestTableFunctionBase extends BuiltInTableFunction<Object> {
        private final transient DataType outputDataType;
        protected final boolean withOrdinality;

        UnnestTableFunctionBase(SpecializedContext context, LogicalType outputType, boolean withOrdinality) {
            super(BuiltInFunctionDefinitions.INTERNAL_UNNEST_ROWS, context);
            this.withOrdinality = withOrdinality;
            // The output type in the context is already wrapped, however, the result of the
            // function is not. Therefore, we need a custom output type.
            if (withOrdinality){
                outputDataType = DataTypes.ROW(
                        DataTypes.FIELD("f0", DataTypes.of(outputType).notNull()),
                        DataTypes.FIELD("ordinality", DataTypes.INT().notNull())
                ).toInternal();
            } else {
                outputDataType = DataTypes.of(outputType).toInternal();
            }
        }

        @Override
        public DataType getOutputDataType() {
            return outputDataType;
        }

        protected Object wrapWithOrdinality(Object value, int ordinal) {
            if (!withOrdinality) {
                return value;
            }
            if (value instanceof GenericRowData) {
                GenericRowData row = (GenericRowData) value;
                Object[] newFields = new Object[row.getArity() + 1];
                for (int i = 0; i < row.getArity(); i++) {
                    newFields[i] = row.getField(i);
                }
                newFields[row.getArity()] = ordinal;
                return GenericRowData.of(newFields);
            } else {
                return GenericRowData.of(value, ordinal);
            }
        }
    }

    /** Table function that unwraps the elements of a collection (array or multiset). */
    public static final class CollectionUnnestTableFunction extends UnnestTableFunctionBase {

        private static final long serialVersionUID = 1L;

        private final ArrayData.ElementGetter elementGetter;

        public CollectionUnnestTableFunction(
                SpecializedContext context,
                LogicalType outputType,
                ArrayData.ElementGetter elementGetter,
                boolean withOrdinality) {
            super(context, outputType, withOrdinality);
            this.elementGetter = elementGetter;
        }

        public void eval(ArrayData arrayData) {
            if (arrayData == null) {
                return;
            }
            final int size = arrayData.size();
            for (int pos = 0; pos < size; pos++) {
                collect(wrapWithOrdinality(elementGetter.getElementOrNull(arrayData, pos), pos + 1));
            }
        }

        public void eval(MapData mapData) {
            if (mapData == null) {
                return;
            }
            final int size = mapData.size();
            final ArrayData keys = mapData.keyArray();
            final ArrayData values = mapData.valueArray();
            int ordinal = 1;
            for (int pos = 0; pos < size; pos++) {
                final int multiplier = values.getInt(pos);
                final Object key = elementGetter.getElementOrNull(keys, pos);
                for (int i = 0; i < multiplier; i++) {
                    collect(wrapWithOrdinality(key, ordinal++));
                }
            }
        }
    }

    /** Table function that unwraps the elements of a map. */
    public static final class MapUnnestTableFunction extends UnnestTableFunctionBase {

        private static final long serialVersionUID = 1L;

        private final ArrayData.ElementGetter keyGetter;
        private final ArrayData.ElementGetter valueGetter;

        public MapUnnestTableFunction(
                SpecializedContext context,
                LogicalType outputType,
                ArrayData.ElementGetter keyGetter,
                ArrayData.ElementGetter valueGetter,
                boolean withOrdinality) {
            super(context, outputType, withOrdinality);
            this.keyGetter = keyGetter;
            this.valueGetter = valueGetter;
        }

        public void eval(MapData mapData) {
            if (mapData == null) {
                return;
            }
            final int size = mapData.size();
            final ArrayData keyArray = mapData.keyArray();
            final ArrayData valueArray = mapData.valueArray();
            for (int i = 0; i < size; i++) {
                collect(
                        wrapWithOrdinality(
                                GenericRowData.of(
                                        keyGetter.getElementOrNull(keyArray, i),
                                        valueGetter.getElementOrNull(valueArray, i)),
                                i + 1));
            }
        }
    }
}
