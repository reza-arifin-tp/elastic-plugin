/*
Based on: https://discuss.elastic.co/t/vector-scoring/85227/4
and https://github.com/MLnick/elasticsearch-vector-scoring

another slower implementation using strings: https://github.com/ginobefun/elasticsearch-feature-vector-scoring

storing arrays is no luck - lucine index doesn't keep the array members orders
https://www.elastic.co/guide/en/elasticsearch/guide/current/complex-core-fields.html

Delimited Payload Token Filter: https://www.elastic.co/guide/en/elasticsearch/reference/2.4/analysis-delimited-payload-tokenfilter.html


 */

package com.tkpatlas.elasticsearch.script;

import com.tkpatlas.elasticsearch.Util;
import java.io.IOException;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.store.ByteArrayDataInput;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.LeafSearchScript;
import org.elasticsearch.script.ScriptException;

import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Map;

/**
 * Script that scores documents based on cosine similarity embedding vectors.
 */
public final class FastLshSimilarityScript implements LeafSearchScript, ExecutableScript {

    public static final String SCRIPT_NAME = "fastlsh";

    private static final int DOUBLE_SIZE = 8;

    // the field containing the vectors to be scored against
    public final String field;

    private int docId;
    private BinaryDocValues binaryEmbeddingReader;

    private final int[] inputHash;
    
    Boolean is_value = false;

    @Override
    public long runAsLong() {
        return ((Number)this.run()).longValue();
    }
    @Override
    public double runAsDouble() {
        return ((Number)this.run()).doubleValue();
    }
    @Override
    public void setNextVar(String name, Object value) {}
    @Override
    public void setDocument(int docId) {
        this.docId = docId;
        // advance has undefined behavior calling with a docid <= its current docid
        is_value = binaryEmbeddingReader != null;
    }

    public void setBinaryEmbeddingReader(BinaryDocValues binaryEmbeddingReader) {
        if(binaryEmbeddingReader == null) {
            is_value = false;
        }
        this.binaryEmbeddingReader = binaryEmbeddingReader;
        is_value = true;
    }


    /**
     * Factory that is registered in
     * {@link FastLshSimilarityPlugin#onModule(org.elasticsearch.script.ScriptModule)}
     * method when the plugin is loaded.
     */
    public static class Factory {

        /**
         * This method is called for every search on every shard.
         * 
         * @param params
         *            list of script parameters passed with the query
         * @return new native script
         */
        public ExecutableScript newScript(@Nullable Map<String, Object> params) throws ScriptException {
            return new FastLshSimilarityScript(params);
        }

        /**
         * Indicates if document scores may be needed by the produced scripts.
         *
         * @return {@code true} if scores are needed.
         */
        public boolean needsScores() {
            return false;
        }

    }

    
    /**
     * Init
     * @param params index that a scored are placed in this parameter. Initialize them here.
     */
    @SuppressWarnings("unchecked")
    public FastLshSimilarityScript(Map<String, Object> params) {
        final Object field = params.get("field");
        if (field == null)
            throw new IllegalArgumentException("Missing parameter [field]");
        this.field = field.toString();
        
        //Get the lsh signature
        final Object hash = params.get("hash");

        //Determine if raw comma-delimited vector or embedding was passed
        if(hash == null) {
            throw new IllegalArgumentException("Must have 'hash' as a parameter");
        } 

        inputHash = Util.convertBase64ToArray((String) hash);
    }


    /**
     * Called for each document
     * @return cosine similarity of the current document against the input inputVector
     */
    @Override
    public final Object run() {
        //If there is no field value return 0 rather than fail.
        if (!is_value) return 0.0d;

        final int inputHashSize = inputHash.length;
        final byte[] bytes;
        bytes = binaryEmbeddingReader.get(docId).bytes;
        if (bytes == null) return 0.0d;
        final ByteArrayDataInput docLsh = new ByteArrayDataInput(bytes);
        docLsh.readVInt();

        final int docLshLength = docLsh.readVInt(); // returns the number of bytes to read

        if(docLshLength != inputHashSize * 4) {
            return 0d;
        }

        final int position = docLsh.getPosition();

        final IntBuffer intBuffer = ByteBuffer.wrap(bytes, position, docLshLength).asIntBuffer();

        final int[] doc_lsh = new int[inputHashSize];
        intBuffer.get(doc_lsh);                          

        double score;                            
        score = 0d;

        int union = 0, intersect = 0;
        for (int i = 0; i < inputHashSize; i ++) {
            intersect += Integer.bitCount(doc_lsh[i] & inputHash[i]);
            union += Integer.bitCount(doc_lsh[i] | inputHash[i]);
        }

        if (union == 0) {
            return score;
        }
        score = ((double)intersect) / union;

        return score;
    }

}