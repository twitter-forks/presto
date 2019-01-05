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
package com.twitter.presto.plugin.eventlistener.slack;

import org.testng.annotations.Test;

import java.util.Optional;
import java.util.regex.Pattern;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestAbsentOrPattern
{
    @Test
    public void testDefaults()
    {
        AbsentOrPattern pattern = new AbsentOrPattern(Optional.empty(), Optional.empty());
        assertTrue(pattern.matches(Optional.empty()));
        assertTrue(pattern.matches(Optional.of("")));
        assertTrue(pattern.matches(Optional.of("user")));
        assertTrue(pattern.matches(Optional.of("user@exmaple.top")));
    }

    @Test
    public void testExplicitMatchAbsentOnly()
    {
        AbsentOrPattern pattern = new AbsentOrPattern(Optional.empty(), Optional.of(Pattern.compile("a^")));
        assertTrue(pattern.matches(Optional.empty()));
        assertFalse(pattern.matches(Optional.of("")));
        assertFalse(pattern.matches(Optional.of("user")));
        assertFalse(pattern.matches(Optional.of("user@exmaple.top")));
    }

    @Test
    public void testExplicitMatchRegexOnly()
    {
        AbsentOrPattern pattern = new AbsentOrPattern(Optional.of(false), Optional.of(Pattern.compile("user.*")));
        assertFalse(pattern.matches(Optional.empty()));
        assertFalse(pattern.matches(Optional.of("")));
        assertTrue(pattern.matches(Optional.of("user")));
        assertTrue(pattern.matches(Optional.of("user@exmaple.top")));
    }

    @Test
    public void testExplicitMatchEither()
    {
        AbsentOrPattern pattern = new AbsentOrPattern(Optional.of(true), Optional.of(Pattern.compile("user.*")));
        assertTrue(pattern.matches(Optional.empty()));
        assertFalse(pattern.matches(Optional.of("")));
        assertTrue(pattern.matches(Optional.of("user")));
        assertTrue(pattern.matches(Optional.of("user@exmaple.top")));
    }
}
