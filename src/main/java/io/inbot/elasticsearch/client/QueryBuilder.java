package io.inbot.elasticsearch.client;

import static com.github.jsonj.tools.JsonBuilder.array;
import static com.github.jsonj.tools.JsonBuilder.field;
import static com.github.jsonj.tools.JsonBuilder.object;
import static com.github.jsonj.tools.JsonBuilder.primitive;

import com.github.jsonj.JsonArray;
import com.github.jsonj.JsonElement;
import com.github.jsonj.JsonObject;
import io.inbot.datemath.DateMath;
import java.time.Instant;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

/**
 * Some factory methods for creating elastic search queries. Note, this class is incomplete and probably does not support the complete es DSL at this point.
 *
 * I'm adding things on a need to have basis. Pull requests welcome.
 */
public class QueryBuilder {
    public static JsonObject matchAll() {
        return object().put("match_all", object().get()).get();
    }

    public static JsonObject boost(JsonObject query, double boost) {
        return object().put("custom_boost_factor", object()
                .put("query", query)
                .put("boost_factor", boost)
                .get()).get();
    }

    public static JsonObject boosting(JsonObject positive, JsonObject negative, double negativeBoost) {
        return object(field("boosting",

                object(field("positive", positive), field("negative", negative), field("negative_boost", negativeBoost))));
    }

    public static JsonObject bool(JsonArray must, JsonArray should, JsonArray mustNot) {
        JsonObject bool = new JsonObject();
        putIfNotEmpty(bool, "must", must);
        putIfNotEmpty(bool, "should", should);
        putIfNotEmpty(bool, "must_not", mustNot);
        return object().put("bool", bool).get();
    }

    public static JsonObject prefixFilter(String field, String prefix) {
        return object(
                field("prefix",object(
                        field(field,prefix))));
    }

    public static JsonObject bool(JsonArray must, JsonArray should, JsonArray mustNot, double boost) {
        JsonObject bool = new JsonObject();
        putIfNotEmpty(bool, "must", must);
        putIfNotEmpty(bool, "should", should);
        putIfNotEmpty(bool, "must_not", mustNot);
        bool.put("boost", boost);
        return object().put("bool", bool).get();
    }

    public static JsonObject bool(JsonArray must, JsonArray should, JsonArray mustNot, boolean disableCoord) {
        JsonObject bool = new JsonObject();
        putIfNotEmpty(bool, "must", must);
        putIfNotEmpty(bool, "should", should);
        putIfNotEmpty(bool, "must_not", mustNot);
        // basically allows each of the subclauses to score 1* score instead of 1/n * score, makes it more like a xor
        bool.put("disable_coord", disableCoord);
        return object().put("bool", bool).get();
    }

    public static JsonObject or(JsonObject... clauses) {
        return or(array(clauses));
    }

    public static JsonObject or(JsonArray clauses) {
        return object(field("or", clauses));
    }

    public static JsonObject and(JsonObject... clauses) {
        return and(array(clauses));
    }

    public static JsonObject and(JsonArray clauses) {
        return object(field("and", clauses));
    }

    public static JsonObject notFilter(JsonObject clause) {
//        return object(field("not", clause));
        return object(field("not", object(field("filter",clause))));
    }

    public static JsonObject disMax(JsonArray queries, double boost, double tieBreaker) {
        return object()
                .put("dis_max", object()
                        .put("boost", boost)
                        .put("tie_breaker", tieBreaker)
                        .put("queries", queries)
                        .get())
                .get();
    }

    public static JsonObject multiMatch(String query, JsonArray fields) {
        return object()
                .put("multiMatch", object()
                        .put("query", query)
                        .put("fields", fields)
                        .get())
                .get();
    }

    public static JsonObject multiMatch(String query, String type, JsonArray fields) {
        return object()
                .put("multiMatch", object()
                        .put("query", query)
                        .put("type", type)
                        .put("fields", fields)
                        .get())
                .get();
    }

    public static JsonObject multiMatch(String query, String type, JsonArray fields, double boost) {
        return object()
                .put("multiMatch", object()
                        .put("query", query)
                        .put("type", type)
                        .put("fields", fields)
                        .put("boost", boost)
                        .get())
                .get();
    }

    public static JsonObject queryWithVersion(JsonObject wrapped) {
        return object(field("query", wrapped), field("version", true));
    }

    public static JsonObject aggregation(String name, JsonObject aggregation) {
        return object(
                field(name, aggregation)
        );
    }

    public static JsonObject aggregationQuery(JsonObject query, JsonObject aggs) {
        return object(
                field("size", 0),
                field("query", query),
                field("aggs", aggs)
            );
    }

    public static JsonObject aggregationQuery(JsonObject aggs) {
        return object(
                field("size", 0),
                field("aggs", aggs)
            );
    }

    /**
     * @param wrapped query
     * @return query suitable for use with delete (doesn't support version).
     */
    public static JsonObject deleteQuery(JsonObject wrapped) {
        return object(field("query", wrapped));
    }

    public static JsonObject query(JsonObject query) {
        if(query != null) {
            return object(field("query", query));
        } else {
            throw new IllegalArgumentException("query can not be empty");
        }

    }

    public static JsonObject query(int from, int size,JsonObject query) {
        JsonObject result = object(
                field("from", from),
                field("size", size),
                field("version", true)
                );
        if(query != null && !query.isEmpty()) {
            result.put("query", query);
        }
        return result;
    }

    public static JsonObject queryAndSortDesc(int from, int size, String sortField,JsonObject query) {
        Validate.notEmpty(sortField);
        JsonObject result = object(
                field("from", from),
                field("size", size),
                field("version", true),
                field("sort",array(
                        object(
                                field(sortField,object(
                                        field("order", "desc"), field("missing","_last"), field("unmapped_type","long")))),
                        primitive("_score")
                    ))
                );
        if(query != null && !query.isEmpty()) {
            result.put("query", query);
        }
        return result;
    }

    public static JsonObject queryAndSortAsc(int from, int size, String sortField,JsonObject query) {
        Validate.notEmpty(sortField);
        JsonObject result = object(
                field("from", from),
                field("size", size),
                field("version", true),
                field("sort",array(
                        object(
                                field(sortField,object(
                                        field("order", "asc"), field("missing","_last"), field("unmapped_type","long")))),
                        primitive("_score")
                    ))
                );
        if(query != null && !query.isEmpty()) {
            result.put("query", query);
        }
        return result;
    }

    public static JsonObject rangeQuery(String fieldName, Instant fromDate, Instant toDate) {
        return rangeQuery(fieldName, fromDate, true, toDate, true);
    }

    public static JsonObject rangeQuery(String fieldName, String fromDate, boolean fromInclusive, String toDate, boolean toInclusive) {
        Instant from=null;
        Instant to=null;
        if(StringUtils.isNotBlank(fromDate)) {
            from =DateMath.parse(fromDate);
        }
        if(StringUtils.isNotBlank(toDate)) {
            to =DateMath.parse(toDate);
        }
        return rangeQuery(fieldName,from,fromInclusive,to,toInclusive);
    }


    public static JsonObject rangeQuery(String fieldName, Instant fromDate, boolean fromInclusive, Instant toDate, boolean toInclusive) {
        JsonObject range = object().get();
        if(fromDate != null) {
            String operator="gt";
            if(fromInclusive) {
                operator="gte";
            }
            range.put(operator, DateMath.formatIsoDate(fromDate));
        }
        if(toDate != null) {
            String operator="lt";
            if(toInclusive) {
                operator="lte";
            }
            range.put(operator, DateMath.formatIsoDate(toDate));
        }
        if(range.size() == 0) {
            throw new IllegalArgumentException("illegal date range should provide valid from or to parameter");
        }

        return object(field("range",
                        object(
                                field(fieldName, range)
                        )
                )
        );
    }


    public static JsonObject fromToFilter(String field, Long from, Long to) {
        if(from == null && to == null) {
            throw new IllegalArgumentException("illegal range");
        }
        JsonObject numericRange=new JsonObject();
        if(from != null) {
            numericRange.put("gte", ""+from);
        }
        if(to != null) {
            numericRange.put("lte", ""+to);
        }
        return object(field("range", object(field(field, numericRange))));
    }

    public static JsonObject fromToFilter(String field, Double from, Double to) {
        if(from == null && to == null) {
            throw new IllegalArgumentException("illegal range");
        }
        JsonObject numericRange=new JsonObject();
        if(from != null) {
            numericRange.put("gte", ""+from);
        }
        if(to != null) {
            numericRange.put("lte", ""+to);
        }
        return object(field("range", object(field(field, numericRange))));
    }

    public static JsonObject fromFilter(String field, String from) {
        return object(
                field("range", object(
                    field(field, object(
                            field("gte", from)
                                ))
                            ))
                );
    }

    public static JsonObject toFilter(String field, String to) {
        return object(
                field("range", object(
                    field(field, object(
                            field("lte", to)
                                ))
                            ))
                );
    }

    public static JsonObject functionScoreQuery(JsonObject query, String scoreMode, JsonObject...functions) {
        //See ES documentation for function_score.
        JsonArray fs = array();
        for(JsonObject f: functions) {
            fs.add(f);
        }
        return object(
                field("function_score",object(
                        field("query",query),
                        field("score_mode", scoreMode),
                        field("functions", fs)
                        )
                )
            );
    }

    public static JsonObject functionScoreFilter(JsonObject filter, JsonObject...functions) {
        JsonArray fs = array();
        for(JsonObject f: functions) {
            fs.add(f);
        }
        return object(
                field("function_score",object(
                        field("filter",filter),
                        field("functions", fs)
                        )
                )
            );
    }

    public static JsonObject dateScoringQueryFunction(JsonObject query, String field, String scale) {
        return functionScoreQuery(query,"multiply",
                // exp seems to work the best
                decayFunction("exp", field, DateMath.now(), scale));
    }

    public static JsonObject decayFunction(String decayFunction, String field, double[] point, String scale) {
        return decayFunction(decayFunction, field, point[1] + "," + point[0], scale);
    }

    public static JsonObject decayFunction(String decayFunction, String field, Instant origin, String scale) {
        return decayFunction(decayFunction, field, DateMath.formatIsoDate(origin), scale);
    }

    public static JsonObject decayFunction(String decayFunction, String field, String origin, String scale) {
        return object(field(decayFunction, object(
                field(field, object(
                        field("origin", origin),
                        field("scale", scale)
                )))));
    }

    public static JsonObject filtered(JsonObject query,JsonElement filter) {
        return object().put("bool", object()
                .put("must",query)
                .put("filter",filter)
                .get()).get();
    }

    public static JsonObject term(String field, String value) {
        return object(field("term", object(
                field(field, value)
        )));
    }

    public static JsonObject term(String field, Enum<?> value) {
        return object(field("term", object(
                field(field, value.name())
        )));
    }

    public static JsonObject ids(JsonArray ids) {
        return object().put("ids", object()
                           .put("values", ids)).get();
    }

	public static JsonObject term(String field, boolean value) {
		return object(field("term", object().put(field, value).get()));
	}

    public static JsonObject term(String field, long value) {
        return object().put("term", object()
                .put(field,value)
                .get()).get();
    }

    public static JsonArray terms(String field, List<String> values) {
        JsonArray result = array();
        if(values !=null) {
            for(String value: values) {
                result.add(term(field,value));
            }
        }
        return result;
    }

    public static JsonObject terms(String field, JsonArray values) {
        return object().put("terms", object()
                        .put(field, values)).get();
    }

    public static JsonObject term(String field, String value, double boost) {
        return object().put("term", object()
                .put(field, object()
                        .put("value", value)
                        .put("boost", boost)
                        .get())
                .get()).get();
    }

    public static JsonObject match(String field, String value,double boost) {
        return object()
                .put("match", object()
                        .put(field, object()
                                .put("query", value)
                                .put("boost", boost)
                                .get())
                        .get()).get();
    }

    public static JsonObject match(String field, String value) {
        return object()
                .put("match", object()
                    .put(field, object()
                            .put("query", value)
                            .get())
                    .get())
                .get();
    }

    public static JsonObject match(String field, String value, String operator) {
        return object()
                .put("match", object()
                        .put(field, object()
                                .put("query", value)
                                .put("operator", operator)
                                .get())
                        .get())
                .get();
    }

    public static JsonObject match(String field, String value, String operator, double boost) {
        return object()
                .put("match", object()
                        .put(field, object()
                                .put("query", value)
                                .put("operator", operator)
                                .put("boost", boost)
                                .get())
                        .get())
                .get();
    }

    public static JsonObject fieldExists(String field) {
    	JsonObject object = new JsonObject();
    	object.put("exists", object().put("field", field).get());
    	return object;
    }
    /**
     * @param field field name
     * @param value value to match
     * @param boost boost to give to the query term
     * @param cutOffFrequency optimization to prevent unnecessary scoring of very frequent terms
     * @param fuzziness levenstein; set to AUTO for sensible default fuzziness
     * @return the query
     */
    public static JsonObject match(String field, String value,double boost, double cutOffFrequency, String fuzziness) {
        return match(field, value,cutOffFrequency, fuzziness,true, boost);
    }

    public static JsonObject match(String field, String value, double cutOffFrequency, String fuzziness, boolean and, double boost) {
        String operator="or";
        if(and) {
            operator="and";
        }
        return object(
                field("match", object(
                        field(field, object(
                                        field("query", value),
                                        field("operator", operator),
                                        field("fuzziness", fuzziness),
                                        field("cutoff_frequency", cutOffFrequency),
                                        field("boost", boost)
                                )
                        )
                )));
    }

    public static JsonObject filterAggregation(String name, JsonObject filter, JsonObject aggs) {
        return object(field(name, object(field("filter", filter), field("aggs", aggs))));
    }

    public static JsonObject matchPhrasePrefix(String field, String value) {
        return object().put("match", object()
                .put(field,object(
                        field("query", value),
                        field("type", "phrase_prefix")
                        ))
                .get()).get();
    }

    public static JsonObject matchPhrasePrefix(String field, String value, double boost) {
        return object().put("match", object()
                .put(field,object(
                        field("query", value),
                        field("type", "phrase_prefix"),
                        field("boost", boost)
                        ))
                .get()).get();
    }

    public static JsonObject missingFieldFilter(String field) {
        return object(field("missing", object(field("field", field))));
    }


    /**
     * @param field field
     * @param value value
     * @param slop allows tokens to be separated by slop amount of tokens
     * @param fuzziness set to a number to allow levenstein edit distance (in number of characters) or set to AUTO to have it adapt on the token length.
     * @return query
     */
    public static JsonObject matchPhrase(String field, String value, int slop, int fuzziness) {
        return matchPhrase(field, value, slop, "" + fuzziness);
    }

    /**
     * @param field field field
     * @param value value value
     * @param slop allows tokens to be separated by slop amount of tokens
     * @param fuzziness set to a number to allow levenstein edit distance (in number of characters) or set to AUTO to have it adapt on the token length.
     * @return the query
     */
    public static JsonObject matchPhrase(String field, String value, int slop, String fuzziness) {
        return object().put("match", object()
                .put(field,object(
                        field("query", value),
                        field("type", "phrase"),
                        field("slop", slop),
                        field("fuzziness", fuzziness)
                        ))
                .get()).get();

    }

    public static JsonObject matchPhrase(String field, String value, int slop, String fuzziness, double boost) {
        return object().put("match", object()
                .put(field,object(
                        field("query", value),
                        field("type", "phrase"),
                        field("slop", slop),
                        field("fuzziness", fuzziness),
                        field("boost", boost)
                        ))
                .get()).get();

    }

    private static void putIfNotEmpty(JsonObject parent, String fieldName, JsonArray array) {
        if(array != null && array.size() > 0) {
            parent.put(fieldName, array);
        }
    }

    public static JsonObject dateHistogramAgg(String field, int minDocCount, String interval, JsonObject subAggs) {
        JsonObject agg=object(
                field("date_histogram", object(
                        field("field",field),
                        field("min_doc_count", minDocCount),
                        field("interval",interval))));
        if(subAggs != null) {
            agg.put("aggs", subAggs);
        }
        return agg;
    }

    public static JsonObject termsAgg(String field, int size, JsonObject subAggs) {
        JsonObject agg = object(
                field("terms", object(
                        field("size",size),
                        field("field",field))));
        if(subAggs != null) {
            agg.put("aggs", subAggs);
        }
        return agg;
    }

    public static JsonObject termsAggShardSize(String field, int size, int shardSize, JsonObject subAggs) {
        JsonObject agg = termsAgg(field, size, subAggs);
        agg.getOrCreateObject("terms").put("shard_size", shardSize);
        return agg;
    }

    public static JsonObject minAgg(String field) {
        return object(field("min", object(field("field", field))));
    }

    public static JsonObject maxAgg(String field) {
        return object(field("max", object(field("field", field))));
    }

    public static JsonObject sumAgg(String field) {
        return object(field("sum", object(field("field", field))));
    }

    public static JsonObject topHits(int size) {
        return object(field("top_hits", object(field("size",size))));
    }

    public static JsonObject topHits(int size, String sortField, boolean ascending) {
        String order="desc";
        if(ascending) {
            order="asc";
        }
        return object(field("top_hits",
                object(
                        field("size",size),
                        field("sort",object(field(sortField, object(field("order",order)))))
                )));
    }

    public static JsonObject statsAggScripted(String script) {
        return object(field("stats", object(field("script", script))));
    }

    public static JsonObject filteredAgg(JsonObject filter, JsonObject subAgg) {
        return object(field("filter", filter), field("aggs",subAgg));
    }

    public static JsonObject aggsQuery(JsonObject aggs) {
        return object(
                field("size",0),
                field("aggs", aggs));
    }

    public static JsonObject aggsQuery(JsonObject query, JsonObject aggs) {
        return object(
                field("size", 0),
                field("query", query),
                field("aggs", aggs));
    }
}
