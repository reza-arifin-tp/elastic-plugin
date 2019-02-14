package com.tkpatlas.elasticsearch.service;

import com.tkpatlas.elasticsearch.script.FastLshSimilarityScript;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.LeafSearchScript;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Map;

/**
 * Created by Lior Knaany on 5/14/17.
 * Updated by Reza on 7/2/19.
 */
public class FastLshSimilarityScriptEngineService extends AbstractComponent implements ScriptEngineService{

    public static final String NAME = "fastlsh";

    @Inject
    public FastLshSimilarityScriptEngineService(Settings settings) {
        super(settings);
    }

    @Override
    public Object compile(String scriptName, String scriptSource, Map<String, String> params) {
        return new FastLshSimilarityScript.Factory();
    }


    @Override
    public boolean isInlineScriptEnabled() {
        return true;
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public String getExtension() {
        return NAME;
    }

    @Override
    public ExecutableScript executable(CompiledScript compiledScript, @Nullable Map<String, Object> vars) {
        FastLshSimilarityScript.Factory scriptFactory = (FastLshSimilarityScript.Factory) compiledScript.compiled();
        return scriptFactory.newScript(vars);
    }

    @Override
    public SearchScript search(CompiledScript compiledScript, final SearchLookup lookup, @Nullable final Map<String, Object> vars) {
        final FastLshSimilarityScript.Factory scriptFactory = (FastLshSimilarityScript.Factory) compiledScript.compiled();
        final FastLshSimilarityScript script = (FastLshSimilarityScript) scriptFactory.newScript(vars);
        return new SearchScript() {
            @Override
            public LeafSearchScript getLeafSearchScript(LeafReaderContext context) throws IOException {
                script.setBinaryEmbeddingReader(context.reader().getBinaryDocValues(script.field));
                return script;
            }
            @Override
            public boolean needsScores() {
                return scriptFactory.needsScores();
            }
        };
    }

    @Override
    public void close() {
    }
}
