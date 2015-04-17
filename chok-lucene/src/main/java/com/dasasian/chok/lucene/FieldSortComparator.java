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
package com.dasasian.chok.lucene;

import com.dasasian.chok.util.WritableType;
import org.apache.lucene.search.SortField;

import java.util.Comparator;

/**
 * Implementation of an {@link Comparator} that compares two {@link Hit} objects
 * based on a given {@link org.apache.lucene.search.Sort} specification. This comparator helps sorting a
 * result list by field terms rather then by sore.
 * <p>
 * This code leans on the lucene code from {@link FieldSortedHitQueue}
 */
@SuppressWarnings("unchecked")
class FieldSortComparator implements Comparator<Hit> {

    private static Comparator<Object> COMPARABLE_COMPARATOR = new ComparableComparator();
    private static Comparator<Object> REVERSED_COMPARABLE_COMPARATOR = new ComparableComparator(true);

    private final SortField[] _sortFields;
    private final WritableType[] _fieldTypes;
    private final Comparator<Object>[] _fieldComparators;

    public FieldSortComparator(SortField[] sortFields, WritableType[] fieldTypes) {
        _sortFields = sortFields;
        _fieldTypes = fieldTypes;
        _fieldComparators = new Comparator[sortFields.length];

        // prepare a array of comparators, for each field one. For type-information
        // we use the user provided SortField[] and the WritableType[] which are
        // auto-detected on the node side.
        for (int i = 0; i < sortFields.length; i++) {
            if (_fieldTypes[i] == WritableType.TEXT && sortFields[i].getLocale() != null) {
                throw new UnsupportedOperationException("locale-sensitive field sort currently not supported");
                // jz: therefore we could use java.text.Collator class (see lucenes
                // FieldSortedHitQueue)
            } else if (sortFields[i].getType() == SortField.CUSTOM) {
                throw new UnsupportedOperationException("custom field sort currently not supported");
            }

            if (_sortFields[i].getType() == SortField.SCORE) {
                _fieldComparators[i] = REVERSED_COMPARABLE_COMPARATOR;
            } else {
                _fieldComparators[i] = COMPARABLE_COMPARATOR;
            }
        }
    }

    public SortField[] getSortFields() {
        return _sortFields;
    }

    public WritableType[] getFieldTypes() {
        return _fieldTypes;
    }

    @Override
    public int compare(Hit hit1, Hit hit2) {
        return compare(hit1.getSortFields(), hit2.getSortFields());
    }

    public int compare(Object[] fields1, Object[] fields2) {
        int n = _sortFields.length;
        int c = 0;
        for (int i = 0; i < n && c == 0; ++i) {
            Object fieldTerm1 = fields1[i];
            Object fieldTerm2 = fields2[i];
            c = (_sortFields[i].getReverse()) ? _fieldComparators[i].compare(fieldTerm2, fieldTerm1) : _fieldComparators[i].compare(fieldTerm1, fieldTerm2);
        }
        return c;
    }

    static class ComparableComparator implements Comparator<Object> {

        private final boolean _reverse;

        public ComparableComparator() {
            this(false);
        }

        public ComparableComparator(boolean reverse) {
            _reverse = reverse;
        }

        @Override
        public int compare(Object o1, Object o2) {
            if (_reverse) {
                return ((Comparable) o2).compareTo(o1);
            }
            return ((Comparable) o1).compareTo(o2);
        }

    }

}
