package com.tkpatlas.elasticsearch;

import com.tkpatlas.elasticsearch.Util;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

/**
 * Created by Lior Knaany on 4/7/18.
 * Updated by Reza on 7/2/19.
 */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
public class TestObject {
    int jobId;
    String embeddingHash;
    int[] vector;

    public int getJobId() {
        return jobId;
    }

    public String getEmbeddingHash() {
        return embeddingHash;
    }

    public int[] getVector() {
        return vector;
    }

    public TestObject(int jobId, int[] vector) {
        this.jobId = jobId;
        this.vector = vector;
        this.embeddingHash = Util.convertArrayToBase64(vector);
    }
}
