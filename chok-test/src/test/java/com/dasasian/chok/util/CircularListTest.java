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

import com.dasasian.chok.testutil.AbstractTest;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class CircularListTest extends AbstractTest {

    @Test
    public void testConstructor() {
        CircularList<Integer> list = new CircularList<>();
        assertEquals(0, list.size());
        assertEquals(null, list.getNext());
        assertEquals(null, list.getTop());
        list.moveToEnd();
        list.moveToMid();
    }

    @Test
    public void testAdd_GetNext() {
        CircularList<Integer> list = new CircularList<>();
        assertEquals(0, list.size());

        int valueCount = 100;
        for (int i = 0; i < valueCount; i++) {
            list.add(i);
        }
        assertEquals(valueCount, list.size());
        assertEquals(new Integer(valueCount - 1), list.getTop());
        assertEquals(new Integer(valueCount - 1), list.getTop());

        for (int i = 0; i < valueCount; i++) {
            assertEquals(new Integer(valueCount - 1 - i), list.getNext());
        }
        for (int i = 0; i < valueCount; i++) {
            assertEquals(new Integer(valueCount - 1 - i), list.getNext());
        }

        list.add(-1);
        assertEquals(new Integer(-1), list.getTop());
        assertEquals(new Integer(-1), list.getNext());
    }

    @Test
    public void testMoveToEnd() {
        CircularList<Integer> list = new CircularList<>();
        int valueCount = 10;
        for (int i = 0; i < valueCount; i++) {
            list.add(i);
        }

        assertEquals(new Integer(valueCount - 1), list.getTop());
        list.moveToEnd();
        assertEquals(new Integer(valueCount - 2), list.getTop());
        for (int i = 0; i < valueCount - 1; i++) {
            list.getNext();
        }
        assertEquals(new Integer(valueCount - 1), list.getTop());

        for (int i = 0; i < valueCount; i++) {
            list.moveToEnd();
        }
        assertEquals(new Integer(valueCount - 1), list.getTop());
    }

    @Test
    public void testMoveToMid() {
        CircularList<Integer> list = new CircularList<>();
        // test just work if valueCount is even
        int valueCount = 10;
        for (int i = 0; i < valueCount; i++) {
            list.add(i);
        }

        assertEquals(new Integer(valueCount - 1), list.getTop());
        list.moveToMid();
        assertEquals(new Integer(valueCount - 2), list.getTop());
        for (int i = 0; i < (valueCount - 1) / 2; i++) {
            list.getNext();
        }
        assertEquals(new Integer(valueCount - 1), list.getTop());

        for (int i = 0; i < (valueCount) / 2; i++) {
            list.moveToMid();
        }
        assertEquals(new Integer(valueCount - 1), list.getTop());
    }

    @Test
    public void testRemoveTop() {
        CircularList<Integer> list = new CircularList<>();
        int valueCount = 10;
        for (int i = 0; i < valueCount; i++) {
            list.add(i);
        }

        assertEquals(new Integer(valueCount - 1), list.getTop());
        list.removeTop();
        assertEquals(new Integer(valueCount - 2), list.getTop());
        for (int i = list.size(); i > 0; i--) {
            assertEquals(new Integer(list.size() - 1), list.removeTop());
        }
        assertEquals(0, list.size());
        assertEquals(null, list.removeTop());
        assertEquals(null, list.getNext());
        assertEquals(null, list.getTop());

        list.moveToEnd();
        list.moveToMid();
    }

    @Test
    public void testRemove() {
        CircularList<Integer> list = new CircularList<>();
        int valueCount = 10;
        for (int i = 0; i < valueCount; i++) {
            list.add(i);
        }

        assertEquals(new Integer(valueCount - 1), list.getTop());
        list.remove(valueCount - 1);
        assertEquals(new Integer(valueCount - 2), list.getTop());

        list.remove(valueCount - 4);
        assertEquals(new Integer(valueCount - 2), list.getNext());
        assertEquals(new Integer(valueCount - 3), list.getNext());
        assertEquals(new Integer(valueCount - 5), list.getNext());

        assertEquals(valueCount - 2, list.size());
        assertEquals(new Integer(valueCount - 6), list.removeTop());
        assertEquals(new Integer(valueCount - 7), list.getTop());
        list.moveToEnd();
        list.moveToMid();
        while (list.size() > 0) {
            list.remove(list.getTop());
        }

        list.remove(0);
    }

    @Test
    public void testGetTopGetTail() {
        CircularList<String> list = new CircularList<>(Arrays.asList("1", "2", "3"));
        assertEquals("1", list.getTop());
        assertEquals("3", list.getTail());
        list.getNext();
        assertEquals("2", list.getTop());
        assertEquals("1", list.getTail());
        list.getNext();
        assertEquals("3", list.getTop());
        assertEquals("2", list.getTail());
        list.getNext();
        assertEquals("1", list.getTop());
        assertEquals("3", list.getTail());
    }
}
