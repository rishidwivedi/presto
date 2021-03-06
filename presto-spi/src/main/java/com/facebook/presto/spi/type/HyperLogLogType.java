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
package com.facebook.presto.spi.type;

import com.facebook.presto.spi.Session;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.block.BlockEncodingFactory;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.block.VariableWidthBlockEncoding;
import com.fasterxml.jackson.annotation.JsonCreator;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;

// Layout is <size>:<hll>, where
//   size: is a short describing the length of the hll bytes
//   hll: is the serialized hll
public class HyperLogLogType
        implements VariableWidthType
{
    public static final HyperLogLogType HYPER_LOG_LOG = new HyperLogLogType();

    public static final BlockEncodingFactory<?> BLOCK_ENCODING_FACTORY = new VariableWidthBlockEncoding.VariableWidthBlockEncodingFactory(HYPER_LOG_LOG);

    public static HyperLogLogType getInstance()
    {
        return HYPER_LOG_LOG;
    }

    @JsonCreator
    public HyperLogLogType()
    {
    }

    @Override
    public String getName()
    {
        return "HyperLogLog";
    }

    @Override
    public Class<?> getJavaType()
    {
        return Slice.class;
    }

    @Override
    public int getLength(Slice slice, int offset)
    {
        return getValueSize(slice, offset) + SIZE_OF_SHORT;
    }

    @Override
    public Slice getSlice(Slice slice, int offset)
    {
        return slice.slice(offset + SIZE_OF_SHORT, getValueSize(slice, offset));
    }

    @Override
    public int writeSlice(SliceOutput sliceOutput, Slice value, int offset, int length)
    {
        sliceOutput.writeShort(length);
        sliceOutput.writeBytes(value, offset, length);
        return length + SIZE_OF_SHORT;
    }

    @Override
    public boolean equalTo(Slice leftSlice, int leftOffset, Slice rightSlice, int rightOffset)
    {
        throw new UnsupportedOperationException("HyperLogLog type is not comparable");
    }

    @Override
    public boolean equalTo(Slice leftSlice, int leftOffset, BlockCursor rightCursor)
    {
        throw new UnsupportedOperationException("HyperLogLog type is not comparable");
    }

    @Override
    public int hash(Slice slice, int offset)
    {
        throw new UnsupportedOperationException("HyperLogLog type is not comparable");
    }

    @Override
    public int compareTo(Slice leftSlice, int leftOffset, Slice rightSlice, int rightOffset)
    {
        throw new UnsupportedOperationException("HyperLogLog type is not ordered");
    }

    @Override
    public void appendTo(Slice slice, int offset, BlockBuilder blockBuilder)
    {
        int length = getValueSize(slice, offset);
        blockBuilder.appendSlice(slice, offset + SIZE_OF_SHORT, length);
    }

    @Override
    public void appendTo(Slice slice, int offset, SliceOutput sliceOutput)
    {
        // copy full value including length
        int length = getLength(slice, offset);
        sliceOutput.writeBytes(slice, offset, length);
    }

    @Override
    public Object getObjectValue(Session session, Slice slice, int offset)
    {
        return "<HyperLogLog>";
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus)
    {
        return new VariableWidthBlockBuilder(this, blockBuilderStatus);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }

    private static int getValueSize(Slice slice, int offset)
    {
        return slice.getShort(offset);
    }

    @Override
    public String toString()
    {
        return getName();
    }
}
