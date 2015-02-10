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

import java.util.*;

public class CollectionUtil {

    // TODO jz: avoid with bidi map?
    public static <K, V> Map<V, List<K>> invertListMap(Map<K, List<V>> key2ValuesMap) {
        One2ManyListMap<V, K> value2KeysMap = new One2ManyListMap<>();
        for (K shardName : key2ValuesMap.keySet()) {
            List<V> nodes = key2ValuesMap.get(shardName);
            for (V node : nodes) {
                value2KeysMap.add(node, shardName);
            }
        }
        return value2KeysMap.asMap();
    }

    public static List<String> getListOfAdded(final Collection<String> oldList, final Collection<String> updatedList) {
        final List<String> addedEntriesList = new ArrayList<>();
        extractAddedEntries(oldList, updatedList, addedEntriesList);
        return addedEntriesList;
    }

    public static Set<String> getSetOfAdded(final Collection<String> oldSet, final Collection<String> updatedSet) {
        final Set<String> addedEntriesSet = new HashSet<>();
        extractAddedEntries(oldSet, updatedSet, addedEntriesSet);
        return addedEntriesSet;
    }

    public static List<String> getListOfRemoved(final Collection<String> oldList, final Collection<String> updatedList) {
        final List<String> removedEntriesList = new ArrayList<>();
        extractRemovedEntries(oldList, updatedList, removedEntriesList);
        return removedEntriesList;
    }

    public static Set<String> getSetOfRemoved(final Collection<String> oldSet, final Collection<String> updatedSet) {
        final Set<String> removedEntriesSet = new HashSet<>();
        extractRemovedEntries(oldSet, updatedSet, removedEntriesSet);
        return removedEntriesSet;
    }

    private static void extractAddedEntries(final Collection<String> oldCollection, final Collection<String> updatedCollection, final Collection<String> addedEntriesCollection) {
        for (final String entry : updatedCollection) {
            if (!oldCollection.contains(entry)) {
                addedEntriesCollection.add(entry);
            }
        }
    }

    private static void extractRemovedEntries(final Collection<String> oldCollection, final Collection<String> updatedCollection, final Collection<String> removedEntriesCollection) {
        for (final String string : oldCollection) {
            if (!updatedCollection.contains(string)) {
                removedEntriesCollection.add(string);
            }
        }
    }

}
