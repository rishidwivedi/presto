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

import com.facebook.presto.spi.Range;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SimpleRange
{
    private final SimpleMarker low;
    private final SimpleMarker high;

    public SimpleRange(Range range)
    {
        this.low = new SimpleMarker(range.getLow());
        this.high = new SimpleMarker(range.getHigh());
    }

    @JsonProperty("low")
    public SimpleMarker getLow()
    {
        return low;
    }

    @JsonProperty("high")
    public SimpleMarker getHigh()
    {
        return high;
    }
}
