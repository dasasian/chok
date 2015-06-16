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

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

/**
 * CircularList where added elements are always on top. <br>
 * You can cycle through the list with getNext(), whereat the returned object is
 * moved to the end of list. <br>
 * Also you could get the top of the list through getTop() and then move it to
 * end with moveToEnd() or to the middle of this list with moveToMid().
 * <p>
 * Note: This implementation is not synchronized.
 */
public class CircularList<E> {

    private final List<E> elements;

    private int currentPos = 0;

    /**
     * Constructs an empty list with the given initial capacity.
     *
     * @param initialCapacity initial capacity of the list
     */
    public CircularList(int initialCapacity) {
        this(new ArrayList<>(initialCapacity));
    }

    /**
     * Constructs an empty list with initial capacity of 10.
     */
    public CircularList() {
        this(10);
    }

    public CircularList(List<E> list) {
        elements = list;
    }

    public CircularList(Iterable<E> list) {
        elements = Lists.newArrayList(list);
    }

    /**
     * Adds the element at top of this list
     *
     * @param element the element to add
     */
    public void add(E element) {
        elements.add(currentPos, element);
    }

    /**
     * @return the top of this list
     */
    public E getTop() {
        if (size() == 0)
            return null;

        return elements.get(currentPos);
    }

    /**
     * @return the tail of this list
     */
    public E getTail() {
        if (size() == 0)
            return null;

        return elements.get(getPreviousPos());
    }

    /**
     * Returns the top of this list and moves it to end.
     *
     * @return the top of this list
     */
    public E getNext() {
        if (size() == 0)
            return null;

        E result = elements.get(currentPos);
        incrementPos();
        return result;
    }

    /**
     * Moves the top of this list to end.
     */
    public void moveToEnd() {
        incrementPos();
    }

    /**
     * Moves the top of this list to the middle.
     */
    public void moveToMid() {
        if (size() == 0)
            return;

        E element = elements.remove(currentPos);
        int midPos = size() / 2 + currentPos;
        if (midPos > size())
            midPos -= size();
        elements.add(midPos, element);
    }

    /**
     * @return the top of this list
     */
    public E removeTop() {
        if (size() == 0)
            return null;

        E top = elements.remove(currentPos);
        if (currentPos == size())
            currentPos = 0;
        return top;
    }

    /**
     * @param element the element to remove
     * @return true if list contained this element
     */
    public boolean remove(E element) {
        boolean contained = elements.remove(element);
        if (currentPos == size())
            currentPos = 0;
        return contained;
    }

    /**
     * @return the number of elements in this list
     */
    public int size() {
        return elements.size();
    }

    public boolean isEmpty() {
        return elements.isEmpty();
    }

    private void incrementPos() {
        currentPos++;
        if (currentPos == elements.size())
            currentPos = 0;
    }

    private int getPreviousPos() {
        if (currentPos == 0) {
            return elements.size() - 1;
        }
        return currentPos - 1;
    }

    @Override
    public String toString() {
        return elements.toString();
    }

    public List<E> asList() {
        return elements;
    }

}
