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
import java.util.stream.Collectors;

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
        return updatedList.stream().filter(entry -> !oldList.contains(entry)).collect(Collectors.toList());
    }


    public static Set<String> getSetOfAdded(final Collection<String> oldSet, final Collection<String> updatedSet) {
        return updatedSet.stream().filter(entry -> !oldSet.contains(entry)).collect(Collectors.toSet());
    }

    public static List<String> getListOfRemoved(final Collection<String> oldList, final Collection<String> updatedList) {
        return oldList.stream().filter(string -> !updatedList.contains(string)).collect(Collectors.toList());
    }

    public static Set<String> getSetOfRemoved(final Collection<String> oldSet, final Collection<String> updatedSet) {
        return oldSet.stream().filter(string -> !updatedSet.contains(string)).collect(Collectors.toSet());
    }

}
