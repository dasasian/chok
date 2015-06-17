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

import com.dasasian.chok.node.IContentServer;
import com.dasasian.chok.util.ClassUtil;
import com.dasasian.chok.util.NodeConfiguration;
import com.dasasian.chok.util.WritableType;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.slf4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.document.MapFieldSelector;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.TimeLimitingCollector.TimeExceededException;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.PriorityQueue;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * The back end server which searches a set of Lucene indices. Each shard is a
 * Lucene index directory.
 * <p>
 * Normal usage is to first call getDocFreqs() to get the global term
 * frequencies, then pass that back in to search(). This way you get uniform
 * scoring across all the nodes / instances of LuceneServer.
 */
@ProtocolInfo(protocolName = "LuceneServer", protocolVersion = 0L)
public class LuceneServer implements IContentServer, ILuceneServer {

    private final static Logger LOG = LoggerFactory.getLogger(LuceneServer.class);

    protected final Map<String, IndexSearcher> searcherByShard = new ConcurrentHashMap<>();
    protected Cache<Filter, CachingWrapperFilter> filterCache;
    protected ExecutorService threadPool;

    protected String nodeName;
    private float timeoutPercentage = 0.75f;
    private ISearcherFactory searcherFactory;

    public LuceneServer() {
    }

    /**
     * Merges the already sorted sub-lists to one big sorted list.
     */
    private static List<Hit> mergeFieldSort(FieldSortComparator comparator, int count, ScoreDoc[][] sortedFieldDocs, String[] shards, String nodeName) {
        int[] arrayPositions = new int[sortedFieldDocs.length];
        final List<Hit> sortedResult = new ArrayList<>(count);

        BitSet listDone = new BitSet(sortedFieldDocs.length);
        for (int subListIndex = 0; subListIndex < arrayPositions.length; subListIndex++) {
            if (sortedFieldDocs[subListIndex].length == 0) {
                listDone.set(subListIndex, true);
            }
        }
        do {
            int fieldDocArrayWithSmallestFieldDoc = -1;
            FieldDoc smallestFieldDoc = null;
            for (int subListIndex = 0; subListIndex < arrayPositions.length; subListIndex++) {
                if (!listDone.get(subListIndex)) {
                    FieldDoc hit = (FieldDoc) sortedFieldDocs[subListIndex][arrayPositions[subListIndex]];
                    if (smallestFieldDoc == null || comparator.compare(hit.fields, smallestFieldDoc.fields) < 0) {
                        smallestFieldDoc = hit;
                        fieldDocArrayWithSmallestFieldDoc = subListIndex;
                    }
                }
            }
            ScoreDoc[] smallestElementList = sortedFieldDocs[fieldDocArrayWithSmallestFieldDoc];
            FieldDoc fieldDoc = (FieldDoc) smallestElementList[arrayPositions[fieldDocArrayWithSmallestFieldDoc]];
            arrayPositions[fieldDocArrayWithSmallestFieldDoc]++;
            final Hit hit = new Hit(shards[fieldDocArrayWithSmallestFieldDoc], nodeName, fieldDoc.score, fieldDoc.doc);
            hit.setSortFields(WritableType.convertComparable(comparator.getFieldTypes(), fieldDoc.fields));
            sortedResult.add(hit);
            if (arrayPositions[fieldDocArrayWithSmallestFieldDoc] >= smallestElementList.length) {
                listDone.set(fieldDocArrayWithSmallestFieldDoc, true);
            }
        } while (sortedResult.size() < count && listDone.cardinality() < arrayPositions.length);
        return sortedResult;
    }

    @Override
    public long getProtocolVersion(final String protocol, final long clientVersion) throws IOException {
        return 0L;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash) throws IOException {
        return null;
    }

    @Override
    public void init(String nodeName, NodeConfiguration nodeConfiguration) {
        LuceneNodeConfiguration luceneNodeConfiguration = (LuceneNodeConfiguration) nodeConfiguration;
        this.nodeName = nodeName;
        searcherFactory = ClassUtil.newInstance(luceneNodeConfiguration.getSearcherFactorClass());
        timeoutPercentage = luceneNodeConfiguration.getTimeoutPercentage();
        threadPool = new ThreadPoolExecutor(luceneNodeConfiguration.getThreadPoolCoreSize(), luceneNodeConfiguration.getThreadPoolMaxSize(), 100L, TimeUnit.MINUTES, new LinkedBlockingQueue<>());

        if (luceneNodeConfiguration.isFilterCacheEnabled()) {
            filterCache = CacheBuilder.newBuilder().expireAfterAccess(10, TimeUnit.MINUTES).maximumSize(luceneNodeConfiguration.isFilterCacheMaxSize()).build();
        }
    }

    public void setSearcherFactory(ISearcherFactory searcherFactory) {
        this.searcherFactory = searcherFactory;
    }

    public String getNodeName() {
        return nodeName;
    }

    public float getTimeoutPercentage() {
        return timeoutPercentage;
    }

    public long getCollectorTimeout(long clientTimeout) {
        return (long) (timeoutPercentage * clientTimeout);
    }

    /**
     * Adds an shard index search for given name to the list of shards
     * MultiSearcher search in.
     *
     * @param shardName the name of the shard
     * @param shardDir  the shard directory
     * @throws IOException when an error occurs
     */
    @Override
    public void addShard(final String shardName, final File shardDir) throws IOException {
        LOG.info("LuceneServer " + nodeName + " got shard " + shardName);
        try {
            IndexSearcher indexSearcher = searcherFactory.createSearcher(shardName, shardDir);
            searcherByShard.put(shardName, indexSearcher);
        } catch (CorruptIndexException e) {
            LOG.error("Error building index for shard " + shardName, e);
            throw e;
        }
    }

    /**
     * Removes a search by given shardName from the list of searchers.
     */
    @Override
    public void removeShard(final String shardName) {
        LOG.info("LuceneServer " + nodeName + " removing shard " + shardName);
        final IndexSearcher remove = searcherByShard.remove(shardName);
        if (remove == null) {
            return; // nothing to do.
        }
        try {
            remove.close();
        } catch (Exception e) {
            LOG.error("LuceneServer " + nodeName + " error removing shard " + shardName, e);
        }
    }

    @Override
    public Collection<String> getShards() {
        return Collections.unmodifiableCollection(searcherByShard.keySet());
    }

    /**
     * Returns the number of documents a shard has.
     *
     * @param shardName name of the shard
     * @return the number of documents in the shard.
     */
    protected int shardSize(String shardName) {
        final IndexSearcher indexSearcher = getSearcherByShard(shardName);
        if (indexSearcher != null) {
            int size = indexSearcher.getIndexReader().numDocs();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Shard '" + shardName + "' has " + size + " docs.");
            }
            return size;
        }
        throw new IllegalArgumentException("Shard '" + shardName + "' unknown");
    }

    /**
     * Returns data about a shard. Currently the only standard key is
     * SHARD_SIZE_KEY. This value will be reported by the listIndexes command. The
     * units depend on the type of server. It is OK to return an empty map or
     * null.
     *
     * @param shardName The name of the shard to measure. This was the name provided in
     *                  addShard().
     * @return a map of key/value pairs which describe the shard.
     * @throws Exception when an error occurs
     */
    @Override
    public Map<String, String> getShardMetaData(String shardName) throws Exception {
        Map<String, String> metaData = new HashMap<>();
        metaData.put(SHARD_SIZE_KEY, Integer.toString(shardSize(shardName)));
        return metaData;
    }

    /**
     * Close all Lucene indices. No further calls will be made after this one.
     */
    @Override
    public void shutdown() throws IOException {
        for (final Searchable searchable : searcherByShard.values()) {
            searchable.close();
        }
        searcherByShard.clear();
    }

    /**
     * Returns the <code>IndexSearcher</code> of the given shardName.
     *
     * @param shardName the name of the shard
     * @return the <code>IndexSearcher</code> of the given shardName
     */
    protected IndexSearcher getSearcherByShard(String shardName) {
        IndexSearcher indexSearcher = searcherByShard.get(shardName);
        if (indexSearcher == null) {
            throw new IllegalStateException("no index-server for shard '" + shardName + "' found - probably undeployed");
        }
        return indexSearcher;
    }

    @Override
    public HitsMapWritable search(QueryWritable query, DocumentFrequencyWritable freqs, String[] shardNames, long timeout) throws IOException {
        return search(query, freqs, shardNames, timeout, Integer.MAX_VALUE);
    }

    @Override
    public HitsMapWritable search(final QueryWritable query, final DocumentFrequencyWritable freqs, final String[] shards, final long timeout, final int count) throws IOException {
        return search(query, freqs, shards, timeout, count, null, null);
    }

    @Override
    public HitsMapWritable search(QueryWritable query, DocumentFrequencyWritable freqs, String[] shards, final long timeout, int count, SortWritable sortWritable) throws IOException {
        return search(query, freqs, shards, timeout, count, sortWritable, null);
    }

    @Override
    public HitsMapWritable search(QueryWritable query, DocumentFrequencyWritable freqs, String[] shards, final long timeout, int count, FilterWritable filterWritable) throws IOException {
        return search(query, freqs, shards, timeout, count, null, filterWritable);
    }

    @Override
    public HitsMapWritable search(QueryWritable query, DocumentFrequencyWritable freqs, String[] shards, final long timeout, int count, SortWritable sortWritable, FilterWritable filterWritable) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("You are searching with the query: '" + query.getQuery() + "'");
        }

        Query luceneQuery = query.getQuery();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Lucene query: " + luceneQuery.toString());
        }

        long completeSearchTime = 0;
        final HitsMapWritable result = new com.dasasian.chok.lucene.HitsMapWritable(getNodeName());
        long start = 0;
        if (LOG.isDebugEnabled()) {
            start = System.currentTimeMillis();
        }
        Sort sort = null;
        if (sortWritable != null) {
            sort = sortWritable.getSort();
        }
        Filter filter = null;
        if (filterWritable != null) {
            filter = filterWritable.getFilter();
        }
        if (filterCache != null && filter != null) {
            CachingWrapperFilter cachedFilter = filterCache.getIfPresent(filter);
            if (cachedFilter == null) {
                cachedFilter = new CachingWrapperFilter(filter);
                filterCache.put(filter, cachedFilter);
            }
            filter = cachedFilter;
        }
        search(luceneQuery, freqs, shards, result, count, sort, timeout, filter);
        if (LOG.isDebugEnabled()) {
            final long end = System.currentTimeMillis();
            LOG.debug("Search took " + (end - start) / 1000.0 + "sec.");
            completeSearchTime += (end - start);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Complete search took " + completeSearchTime / 1000.0 + "sec.");
            final DataOutputBuffer buffer = new DataOutputBuffer();
            result.write(buffer);
            LOG.debug("Result size to transfer: " + buffer.getLength());
        }
        return result;
    }

    @Override
    public DocumentFrequencyWritable getDocFreqs(final QueryWritable input, final String[] shards) throws IOException {
        Query luceneQuery = input.getQuery();
        final Query rewrittenQuery = rewrite(luceneQuery, shards);
        final DocumentFrequencyWritable docFreqs = new DocumentFrequencyWritable();

        final HashSet<Term> termSet = new HashSet<>();
        rewrittenQuery.extractTerms(termSet);
        for (final String shard : shards) {
            final java.util.Iterator<Term> termIterator = termSet.iterator();
            IndexSearcher searcher = getSearcherByShard(shard);
            while (termIterator.hasNext()) {
                final Term term = termIterator.next();
                final int docFreq = searcher.docFreq(term);
                docFreqs.put(term.field(), term.text(), docFreq);
            }
            docFreqs.addNumDocs(shardSize(shard));
        }
        return docFreqs;
    }

    @Override
    public MapWritable getDetails(final String[] shards, final int docId) throws IOException {
        return getDetails(shards, docId, null);
    }

    @Override
    public MapWritable getDetails(final String[] shards, final int docId, final String[] fieldNames) throws IOException {
        final MapWritable result = new MapWritable();
        final Document doc = doc(shards[0], docId, fieldNames);
        final List<Fieldable> fields = doc.getFields();
        for (final Fieldable field : fields) {
            final String name = field.name();
            if (field.isBinary()) {
                final byte[] binaryValue = field.getBinaryValue();
                result.put(new Text(name), new BytesWritable(binaryValue));
            } else {
                final String stringValue = field.stringValue();
                result.put(new Text(name), new Text(stringValue));
            }
        }
        return result;
    }

    @Override
    public int getResultCount(final QueryWritable query, final String[] shards, long timeout) throws IOException {
        final DocumentFrequencyWritable docFreqs = getDocFreqs(query, shards);
        return search(query, docFreqs, shards, timeout, 1).getTotalHits();
    }

    @Override
    public int getResultCount(final QueryWritable query, FilterWritable filter, final String[] shards, long timeout) throws IOException {
        final DocumentFrequencyWritable docFreqs = getDocFreqs(query, shards);
        return search(query, docFreqs, shards, timeout, 1, null, filter).getTotalHits();
    }

    /**
     * Search in the given shards and return max hits for given query
     *
     * @param query the query
     * @param freqs document frequency writer
     * @param shards the shards
     * @param result the writable for the result
     * @param max max results
     * @param sort the sort order
     * @param timeout timeout value
     * @param filter filter to apply
     * @throws IOException when an error occurs
     */
    protected final void search(final Query query, final DocumentFrequencyWritable freqs, final String[] shards,
                                final HitsMapWritable result, final int max, Sort sort, long timeout, Filter filter) throws IOException {
        timeout = getCollectorTimeout(timeout);
        final Query rewrittenQuery = rewrite(query, shards);
        final int numDocs = freqs.getNumDocsAsInteger();
        final Weight weight = rewrittenQuery.weight(new CachedDfSource(freqs.getAll(), numDocs, new DefaultSimilarity()));
        int totalHits = 0;
        final int shardsCount = shards.length;

        // Run the search in parallel on the shards with a thread pool.
        CompletionService<SearchResult> csSearch = new ExecutorCompletionService<>(threadPool);

        for (int i = 0; i < shardsCount; i++) {
            SearchCall call = new SearchCall(shards[i], weight, max, sort, timeout, i, filter);
            csSearch.submit(call);
        }

        final ScoreDoc[][] scoreDocs = new ScoreDoc[shardsCount][];
        ScoreDoc scoreDocExample = null;
        for (String shard : shards) {
            try {
                final SearchResult searchResult = csSearch.take().get();
                final int callIndex = searchResult.getSearchCallIndex();

                totalHits += searchResult._totalHits;
                scoreDocs[callIndex] = searchResult._scoreDocs;
                if (scoreDocExample == null && scoreDocs[callIndex].length > 0) {
                    scoreDocExample = scoreDocs[callIndex][0];
                }
            } catch (InterruptedException e) {
                throw new IOException("Multithread shard search interrupted:", e);
            } catch (ExecutionException e) {
                throw new IOException("Multithread shard search could not be executed:", e);
            }
        }

        result.addTotalHits(totalHits);

        final Iterable<Hit> finalHitList;
        // Limit the request to the number requested or the total number of
        // documents, whichever is smaller.
        int limit = Math.min(numDocs, max);
        if (sort == null || totalHits == 0) {
            final ChokHitQueue hq = new ChokHitQueue(limit);
            int pos = 0;
            BitSet done = new BitSet(shardsCount);
            while (done.cardinality() != shardsCount) {
                ScoreDoc scoreDoc = null;
                for (int i = 0; i < shardsCount; i++) {
                    // only process this shard if it is not yet done.
                    if (!done.get(i)) {
                        final ScoreDoc[] docs = scoreDocs[i];
                        if (pos < docs.length) {
                            scoreDoc = docs[pos];
                            final Hit hit = new Hit(shards[i], getNodeName(), scoreDoc.score, scoreDoc.doc);
                            if (!hq.insert(hit)) {
                                // no doc left that has a higher score than the lowest score in
                                // the queue
                                done.set(i, true);
                            }
                        } else {
                            // no docs left in this shard
                            done.set(i, true);
                        }
                    }
                }
                // we always wait until we got all hits from this position in all
                // shards.

                pos++;
                if (scoreDoc == null) {
                    // we do not have any more data
                    break;
                }
            }
            finalHitList = hq;
        } else {
            WritableType[] sortFieldsTypes;
            FieldDoc fieldDoc = (FieldDoc) scoreDocExample;
            sortFieldsTypes = WritableType.detectWritableTypes(fieldDoc.fields);
            result.setSortFieldTypes(sortFieldsTypes);
            finalHitList = mergeFieldSort(new FieldSortComparator(sort.getSort(), sortFieldsTypes), limit, scoreDocs, shards, getNodeName());
        }

        for (Hit hit : finalHitList) {
            if (hit != null) {
                result.addHit(hit);
            }
        }
    }

    /**
     * Returns a specified lucene document from a given shard where all or only
     * the given fields are loaded from the index.
     *
     * @param shardName the name of the shard
     * @param docId the document id
     * @param fieldNames the names of the fields
     * @return the document
     * @throws IOException when an error occurs
     */
    protected Document doc(final String shardName, final int docId, final String[] fieldNames) throws IOException {
        final Searchable searchable = getSearcherByShard(shardName);
        if (fieldNames == null) {
            return searchable.doc(docId);
        } else {
            return searchable.doc(docId, new MapFieldSelector(fieldNames));
        }
    }

    /**
     * Rewrites a query for the given shards
     *
     * @param original the original query
     * @param shardNames the names of the shards
     * @return the rewritten query
     * @throws IOException when an error occurs
     */
    protected Query rewrite(final Query original, final String[] shardNames) throws IOException {
        final Query[] queries = new Query[shardNames.length];
        for (int i = 0; i < shardNames.length; i++) {
            final String shard = shardNames[i];
            final IndexSearcher searcher = getSearcherByShard(shard);
            if (searcher == null) {
                throw new IllegalStateException("no index-server for shard '" + shard + "' found - probably undeployed");
            } else {
                queries[i] = searcher.rewrite(original);
            }
        }
        if (queries.length > 0 && queries[0] != null) {
            return queries[0].combine(queries);
        } else {
            LOG.error("No queries available for shards: " + Arrays.toString(shardNames));
        }
        return original;
    }

    protected static class SearchResult {

        protected final int _totalHits;
        protected final ScoreDoc[] _scoreDocs;
        protected int _searchCallIndex;

        public SearchResult(int totalHits, ScoreDoc[] scoreDocs, int searchCallIndex) {
            _totalHits = totalHits;
            _scoreDocs = scoreDocs;
            _searchCallIndex = searchCallIndex;
        }

        public int getTotalHits() {
            return _totalHits;
        }

        public ScoreDoc[] getScoreDocs() {
            return _scoreDocs;
        }

        public int getSearchCallIndex() {
            return _searchCallIndex;
        }

    }

    /**
     * Document Frequency cache acting as a Dummy-Searcher. This class is not a
     * fully-fledged Searcher, but only supports the methods necessary to
     * initialize Weights.
     */
    protected static class CachedDfSource extends Searcher {

        private final Map<TermWritable, Integer> dfMap; // Map from Terms to
        // corresponding doc freqs.

        private final int maxDoc; // Document count.

        public CachedDfSource(final Map<TermWritable, Integer> dfMap, final int maxDoc, final Similarity similarity) {
            this.dfMap = dfMap;
            this.maxDoc = maxDoc;
            setSimilarity(similarity);
        }

        @Override
        public int docFreq(final Term term) {
            int df;
            try {
                df = dfMap.get(new TermWritable(term.field(), term.text()));
            } catch (final NullPointerException e) {
                throw new IllegalArgumentException("df for term " + term.text() + " not available in df-map:" + dfMap, e);
            }
            return df;
        }

        @Override
        public int[] docFreqs(final Term[] terms) {
            final int[] result = new int[terms.length];
            for (int i = 0; i < terms.length; i++) {
                result[i] = docFreq(terms[i]);
            }
            return result;
        }

        @Override
        public int maxDoc() {
            return maxDoc;
        }

        @Override
        public Query rewrite(final Query query) {
            // this is a bit of a hack. We know that a query which
            // creates a Weight based on this Dummy-Searcher is
            // always already rewritten (see preparedWeight()).
            // Therefore we just return the unmodified query here
            return query;
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Document doc(final int i) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Document doc(final int i, final FieldSelector fieldSelector) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Explanation explain(final Weight weight, final int doc) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void search(final Weight weight, final Filter filter, final Collector hitCollector) {
            throw new UnsupportedOperationException();
        }

        @Override
        public TopDocs search(final Weight weight, final Filter filter, final int n) {
            throw new UnsupportedOperationException();
        }

        @Override
        public TopFieldDocs search(final Weight weight, final Filter filter, final int n, final Sort sort) {
            throw new UnsupportedOperationException();
        }
    }

    // Cached document frequency source from apache lucene
    // MultiSearcher.

    protected static class ChokHitQueue extends PriorityQueue<Hit> implements Iterable<Hit> {

        private final int _maxSize;

        ChokHitQueue(final int maxSize) {
            _maxSize = maxSize;
            initialize(maxSize);
        }

        public boolean insert(Hit hit) {
            if (size() < _maxSize) {
                add(hit);
                return true;
            }
            if (lessThan(top(), hit)) {
                insertWithOverflow(hit);
                return true;
            }
            return false;
        }

        @Override
        protected final boolean lessThan(final Hit hitA, final Hit hitB) {
            if (hitA.getScore() == hitB.getScore()) {
                // todo this of source do not work since we have same shardKeys
                // (should we increment docIds?)
                return hitA.getDocId() < hitB.getDocId();
            }
            return hitA.getScore() < hitB.getScore();
        }

        @Override
        public Iterator<Hit> iterator() {
            return new Iterator<Hit>() {
                @Override
                public boolean hasNext() {
                    return ChokHitQueue.this.size() > 0;
                }

                @Override
                public Hit next() {
                    return ChokHitQueue.this.pop();
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException("Can't remove using this iterator");
                }
            };
        }
    }

    /**
     * Implements a single thread of a search. Each shard has a separate
     * SearchCall and they are run more or less in parallel.
     */
    protected class SearchCall implements Callable<SearchResult> {

        protected final String _shardName;
        protected final Weight _weight;
        protected final int _limit;
        protected final Sort _sort;
        protected final long _timeout;
        protected final int _callIndex;
        protected final Filter _filter;

        public SearchCall(String shardName, Weight weight, int limit, Sort sort, long timeout, int callIndex, Filter filter) {
            _shardName = shardName;
            _weight = weight;
            _limit = limit;
            _sort = sort;
            _timeout = timeout;
            _callIndex = callIndex;
            _filter = filter;
        }

        @Override
        @SuppressWarnings({"rawtypes"})
        public SearchResult call() throws Exception {
            final IndexSearcher indexSearcher = getSearcherByShard(_shardName);
            int nDocs = Math.min(_limit, indexSearcher.maxDoc());

            TopDocsCollector resultCollector;
            if (_sort != null) {
                boolean fillFields = true;// see IndexSearcher#search(...)
                boolean fieldSortDoTrackScores = false;
                boolean fieldSortDoMaxScore = false;
                resultCollector = TopFieldCollector.create(_sort, nDocs, fillFields, fieldSortDoTrackScores, fieldSortDoMaxScore, !_weight.scoresDocsOutOfOrder());
            } else {
                resultCollector = TopScoreDocCollector.create(nDocs, !_weight.scoresDocsOutOfOrder());
            }
            try {
                indexSearcher.search(_weight, _filter, wrapInTimeoutCollector(resultCollector));
            } catch (TimeExceededException e) {
                LOG.warn("encountered exceeded timout for query '" + _weight.getQuery() + " on shard '" + _shardName + "' with timeout set to '" + _timeout + "'");
            }
            TopDocs docs = resultCollector.topDocs();
            return new SearchResult(docs.totalHits, docs.scoreDocs, _callIndex);
        }

        @SuppressWarnings({"rawtypes"})
        private Collector wrapInTimeoutCollector(TopDocsCollector resultCollector) {
            if (_timeout <= 0) {
                return resultCollector;
            }
            return new TimeLimitingCollector(resultCollector, Counter.newCounter(), _timeout);
        }
    }

}
