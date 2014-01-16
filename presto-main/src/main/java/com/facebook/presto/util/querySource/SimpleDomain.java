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

import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.Range;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class SimpleDomain
{
    private final boolean nullAllowed;
    private final boolean isNone;
    private final List<SimpleRange> ranges;

    public SimpleDomain(Domain domain)
    {
        if (domain == null)
        {
            this.nullAllowed = true;
            this.isNone = false;
            this.ranges = ImmutableList.<SimpleRange>of();
        }
        else {
            this.nullAllowed = domain.isNullAllowed();
            this.isNone = domain.isNone();
            ImmutableList.Builder<SimpleRange> rangesBuilder = ImmutableList.builder();
            // If predicate is All, there won't be any range, list will be empty
            if (!domain.isAll()) {
                for (Range range : domain.getRanges()) {
                    rangesBuilder.add(new SimpleRange(range));
                }
            }
            this.ranges = rangesBuilder.build();
        }
    }

    @JsonProperty("nullAllowed")
    public boolean isNullAllowed()
    {
        return nullAllowed;
    }

    @JsonProperty("isNone")
    public boolean isNone()
    {
        return isNone;
    }

    @JsonProperty("ranges")
    public List<SimpleRange> getRanges()
    {
        return ranges;
    }
}
