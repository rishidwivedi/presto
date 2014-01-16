/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.util.querySource;

import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.Marker;
import com.facebook.presto.spi.SerializableNativeValue;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.io.IOException;

@JsonSerialize(using = SimpleMarker.Serializer.class)
public final class SimpleMarker
{
    private final Marker marker;

    public SimpleMarker(
            Marker marker
    )
    {
        this.marker = marker;
    }

    public Marker getMarker()
    {
        return marker;
    }

    public static class Serializer
            extends JsonSerializer<SimpleMarker>
    {
        @Override
        public void serialize(SimpleMarker simpleMarker, JsonGenerator generator, SerializerProvider provider)
                throws IOException
        {
            generator.writeStartObject();
            if (simpleMarker.getMarker().isLowerUnbounded() || simpleMarker.getMarker().isUpperUnbounded()) {
                generator.writeStringField("bound", "UNBOUND");
            }
            else {
                generator.writeStringField("bound", simpleMarker.getMarker().getBound().name());
            }
            // If marker is unbound, don't encode value
            if (!(simpleMarker.getMarker().isLowerUnbounded() || simpleMarker.getMarker().isUpperUnbounded())) {
                generator.writeFieldName("value");
                writeValue(simpleMarker.getMarker().getSerializableNativeValue(), generator);
            }
            generator.writeEndObject();
        }

        private static void writeValue(SerializableNativeValue value, JsonGenerator jsonGenerator)
                throws IOException
        {
            ColumnType columnType = ColumnType.fromNativeType(value.getType());
            switch (columnType) {
                case STRING:
                    jsonGenerator.writeString((String) value.getValue());
                    break;
                case BOOLEAN:
                    jsonGenerator.writeBoolean((Boolean) value.getValue());
                    break;
                case LONG:
                    jsonGenerator.writeNumber((Long) value.getValue());
                    break;
                case DOUBLE:
                    jsonGenerator.writeNumber((Double) value.getValue());
                    break;
                default:
                    throw new AssertionError("Unknown type: " + columnType);
            }
        }
    }
}
