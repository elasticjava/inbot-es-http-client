package io.inbot.elasticsearch.jsonclient;

import static com.github.jsonj.tools.JsonBuilder.field;
import static com.github.jsonj.tools.JsonBuilder.object;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.github.jillesvangurp.urlbuilder.UrlBuilder;
import com.github.jsonj.JsonObject;
import com.github.jsonj.exceptions.JsonParseException;
import com.github.jsonj.tools.JsonParser;
import io.inbot.elasticsearch.exceptions.EsBadRequestException;
import io.inbot.elasticsearch.exceptions.EsConnectionException;
import io.inbot.elasticsearch.exceptions.EsUnavailableException;
import io.inbot.elasticsearch.exceptions.EsVersionConflictException;
import io.inbot.utils.MdcContext;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Json rest client for services that expect and return json. Intended for Elasticsearch but could probably be used for
 * other things as well. This client depends on jsonj for parsing and serializing and expects and returns JsonObject instances.
 *
 * This class implements MetricSet and exposes several metrics that you may want to add monitoring for.
 */
public class JsonJRestClient implements MetricSet {
    private static final Logger LOG = LoggerFactory.getLogger(JsonJRestClient.class);
    private final EndpointProvider endPointProvider;
    private final HttpClient httpClient;
    private final JsonParser parser;
    private final Timer requestTimer = new Timer();

    private boolean verbose=false;

    /**
     * Convenience method for use in tests etc. For production use, please use a sensible httpclient setup and endpoint
     * provider strategy appropriate to your environment.
     *
     * @param endpoint
     *            the endpoint
     * @return a simple client with the default httpclient, a simple endpoint provider, and a simple endpoint validation
     *         method that always returns true
     */
    public static JsonJRestClient simpleClient(String endpoint) {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        ThreadAffinityEndPointProvider provider = new ThreadAffinityEndPointProvider(u -> true, new String[] {endpoint});
        return new JsonJRestClient(provider, httpClient, new JsonParser());
    }

    public JsonJRestClient(EndpointProvider endPointProvider, HttpClient httpClient, JsonParser parser) {
        this.endPointProvider = endPointProvider;
        this.httpClient = httpClient;
        this.parser = parser;
    }

    /**
     * You can temporarily log all requests/responses by setting this flag to true. Defaults to false.
     * Note. should not be used in production.
     * @param verbose do you like spam ?
     */
    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    @Override
    public Map<String, Metric> getMetrics() {
        HashMap<String, Metric> metrics = new HashMap<>();
        metrics.put("requests", requestTimer);
        return metrics;
    }

    /**
     * @param path relative path on the endpoint provided by the {@link EndpointProvider}
     * @return an optional of the json response from the server or empty in case of a 404.
     * @throws EsConnectionException
     *             if service is not available (status 429, 502, or 503) or if there is some IO error. In both cases the
     *             endpoint is marked as failed on the EndPointProvider.
     * @throws EsBadRequestException
     *             if the service returns a 400
     * @throws EsVersionConflictException
     *             if the service returns a 409
     * @throws IllegalStateException
     *             if the service responds in an unexpected way
     */
    public Optional<JsonObject> get(String path) {
        return execute(path, u -> new HttpGetWithBody(u), null);
    }

    /**
     * @param path relative path on the endpoint provided by the {@link EndpointProvider}
     * @param payload json object
     * @return an optional of the json response from the server or empty in case of a 404.
     * @throws EsConnectionException
     *             if service is not available (status 429, 502, or 503) or if there is some IO error. In both cases the
     *             endpoint is marked as failed on the EndPointProvider.
     * @throws EsBadRequestException
     *             if the service returns a 400
     * @throws EsVersionConflictException
     *             if the service returns a 409
     * @throws IllegalStateException
     *             if the service responds in an unexpected way
     */
    public Optional<JsonObject> get(String path, JsonObject payload) {
        return execute(path, u -> new HttpGetWithBody(u), payload);
    }

    /**
     * @param path relative path on the endpoint provided by the {@link EndpointProvider}
     * @param payload string
     * @return an optional of the json response from the server or empty in case of a 404.
     * @throws EsConnectionException
     *             if service is not available (status 429, 502, or 503) or if there is some IO error. In both cases the
     *             endpoint is marked as failed on the EndPointProvider.
     * @throws EsBadRequestException
     *             if the service returns a 400
     * @throws EsVersionConflictException
     *             if the service returns a 409
     * @throws IllegalStateException
     *             if the service responds in an unexpected way
     */
    public Optional<JsonObject> get(String path, String payload) {
        return execute(path, u -> new HttpGetWithBody(u), payload);
    }

    /**
     * @param path relative path on the endpoint provided by the {@link EndpointProvider}
     * @return an optional of the json response from the server or empty in case of a 404.
     * @throws EsConnectionException
     *             if service is not available (status 429, 502, or 503) or if there is some IO error. In both cases the
     *             endpoint is marked as failed on the EndPointProvider.
     * @throws EsBadRequestException
     *             if the service returns a 400
     * @throws EsVersionConflictException
     *             if the service returns a 409
     * @throws IllegalStateException
     *             if the service responds in an unexpected way
     */
    public Optional<JsonObject> delete(String path) {
        return execute(path, u -> new HttpDeleteWithBody(u), null);
    }

    /**
     * @param path relative path on the endpoint provided by the {@link EndpointProvider}
     * @param payload json object
     * @return an optional of the json response from the server or empty in case of a 404.
     * @throws EsConnectionException
     *             if service is not available (status 429, 502, or 503) or if there is some IO error. In both cases the
     *             endpoint is marked as failed on the EndPointProvider.
     * @throws EsBadRequestException
     *             if the service returns a 400
     * @throws EsVersionConflictException
     *             if the service returns a 409
     * @throws IllegalStateException
     *             if the service responds in an unexpected way
     */
    public Optional<JsonObject> delete(String path, JsonObject payload) {
        return execute(path, u -> new HttpDeleteWithBody(u), payload);
    }

    /**
     * @param path relative path on the endpoint provided by the {@link EndpointProvider}
     * @param payload string
     * @return an optional of the json response from the server or empty in case of a 404.
     * @throws EsConnectionException
     *             if service is not available (status 429, 502, or 503) or if there is some IO error. In both cases the
     *             endpoint is marked as failed on the EndPointProvider.
     * @throws EsBadRequestException
     *             if the service returns a 400
     * @throws EsVersionConflictException
     *             if the service returns a 409
     * @throws IllegalStateException
     *             if the service responds in an unexpected way
     */
    public Optional<JsonObject> delete(String path, String payload) {
        return execute(path, u -> new HttpDeleteWithBody(u), payload);
    }

    /**
     * @param path relative path on the endpoint provided by the {@link EndpointProvider}
     * @return an optional of the json response from the server or empty in case of a 404.
     * @throws EsConnectionException
     *             if service is not available (status 429, 502, or 503) or if there is some IO error. In both cases the
     *             endpoint is marked as failed on the EndPointProvider.
     * @throws EsBadRequestException
     *             if the service returns a 400
     * @throws EsVersionConflictException
     *             if the service returns a 409
     * @throws IllegalStateException
     *             if the service responds in an unexpected way
     */
    public Optional<JsonObject> put(String path) {
        return execute(path,u -> new HttpPut(u), null);
    }

    /**
     * @param path relative path on the endpoint provided by the {@link EndpointProvider}
     * @param payload json object
     * @return an optional of the json response from the server or empty in case of a 404.
     * @throws EsConnectionException
     *             if service is not available (status 429, 502, or 503) or if there is some IO error. In both cases the
     *             endpoint is marked as failed on the EndPointProvider.
     * @throws EsBadRequestException
     *             if the service returns a 400
     * @throws EsVersionConflictException
     *             if the service returns a 409
     * @throws IllegalStateException
     *             if the service responds in an unexpected way
     */
    public Optional<JsonObject> put(String path, JsonObject payload) {
        return execute(path,u -> new HttpPut(u), payload);
    }

    /**
     * @param path relative path on the endpoint provided by the {@link EndpointProvider}
     * @param payload string
     * @return an optional of the json response from the server or empty in case of a 404.
     * @throws EsConnectionException
     *             if service is not available (status 429, 502, or 503) or if there is some IO error. In both cases the
     *             endpoint is marked as failed on the EndPointProvider.
     * @throws EsBadRequestException
     *             if the service returns a 400
     * @throws EsVersionConflictException
     *             if the service returns a 409
     * @throws IllegalStateException
     *             if the service responds in an unexpected way
     */
    public Optional<JsonObject> put(String path, String payload) {
        return execute(path,u -> new HttpPut(u), payload);
    }

    /**
     * @param path relative path on the endpoint provided by the {@link EndpointProvider}
     * @return an optional of the json response from the server or empty in case of a 404.
     * @throws EsConnectionException
     *             if service is not available (status 429, 502, or 503) or if there is some IO error. In both cases the
     *             endpoint is marked as failed on the EndPointProvider.
     * @throws EsBadRequestException
     *             if the service returns a 400
     * @throws EsVersionConflictException
     *             if the service returns a 409
     * @throws IllegalStateException
     *             if the service responds in an unexpected way
     */
    public Optional<JsonObject> post(String path) {
        return execute(path,u -> new HttpPost(u), null);
    }

    /**
     * @param path relative path on the endpoint provided by the {@link EndpointProvider}
     * @param payload json object
     * @return an optional of the json response from the server or empty in case of a 404.
     * @throws EsConnectionException
     *             if service is not available (status 429, 502, or 503) or if there is some IO error. In both cases the
     *             endpoint is marked as failed on the EndPointProvider.
     * @throws EsBadRequestException
     *             if the service returns a 400
     * @throws EsVersionConflictException
     *             if the service returns a 409
     * @throws IllegalStateException
     *             if the service responds in an unexpected way
     */
    public Optional<JsonObject> post(String path, JsonObject payload) {
        return execute(path,u -> new HttpPost(u), payload);
    }

    /**
     * @param path relative path on the endpoint provided by the {@link EndpointProvider}
     * @param payload string
     * @return an optional of the json response from the server or empty in case of a 404.
     * @throws EsConnectionException
     *             if service is not available (status 429, 502, or 503) or if there is some IO error. In both cases the
     *             endpoint is marked as failed on the EndPointProvider.
     * @throws EsBadRequestException
     *             if the service returns a 400
     * @throws EsVersionConflictException
     *             if the service returns a 409
     * @throws IllegalStateException
     *             if the service responds in an unexpected way
     */
    public Optional<JsonObject> post(String path, String payload) {
        return execute(path,u -> new HttpPost(u), payload);
    }

    private Optional<JsonObject> execute(String path, Function<String, HttpEntityEnclosingRequestBase> reqFactory, Object content) {
        String endPoint = endPointProvider.endPoint();
        String url=UrlBuilder.url(endPoint).append(false, path).build();
        HttpEntityEnclosingRequestBase req = reqFactory.apply(url);
        try(Context timerContext = requestTimer.time()) {
            try(MdcContext mdcContext = MdcContext.create()) {
                mdcContext.put("jsonrestclient_method", req.getMethod());
                mdcContext.put("jsonrestclient_endpoint", endPoint);
                if(content != null) {
                    AbstractHttpEntity entity = new AbstractHttpEntity() {

                        @Override
                        public boolean isRepeatable() {
                            return false;
                        }

                        @Override
                        public long getContentLength() {
                            return -1;
                        }

                        @Override
                        public boolean isStreaming() {
                            return false;
                        }

                        @Override
                        public InputStream getContent() throws IOException {
                            throw new UnsupportedOperationException("use writeTo");
                        }

                        @Override
                        public void writeTo(final OutputStream outstream) throws IOException {
                            if(content instanceof JsonObject) {
                                ((JsonObject)content).serialize(outstream);
                            } else {
                                outstream.write(content.toString().getBytes(StandardCharsets.UTF_8));
                            }
                            outstream.flush();
                        }
                    };
                    entity.setContentType("application/json; charset=utf-8");
                    req.setEntity(entity);
                    return httpClient.execute(req, new JsonObjectRestResponseHandler(mdcContext,req, content));
                } else {
                    return httpClient.execute(req, new JsonObjectRestResponseHandler(mdcContext,req, null));
                }
            } catch (EsUnavailableException | IOException e) {
                // mark the endpoint as failed
                endPointProvider.failEndpoint(endPoint);
                throw new EsConnectionException("could not execute "+req.getMethod()+" to " + req.getURI() +" " + e.getMessage(), e);
            }
        }
    }

    /**
     * HttpClient response handler that processes the response and extracts the json object or provides sane error handling.
     */
    private final class JsonObjectRestResponseHandler implements ResponseHandler<Optional<JsonObject>> {
        private final HttpEntityEnclosingRequestBase request;
        private final Object payload; // Object because we sometimes pass a string instead of a json object :-)
        private final MdcContext mdcContext;

        public JsonObjectRestResponseHandler(MdcContext mdcContext, HttpEntityEnclosingRequestBase request, Object payload) {
            this.mdcContext = mdcContext;
            this.request = request;
            this.payload = payload;
        }

        @Override
        public Optional<JsonObject> handleResponse(HttpResponse response) throws IOException {
            int statusCode = response.getStatusLine().getStatusCode();
            mdcContext.put("jsonrestclient_status", statusCode);
            if (statusCode >= 200 && statusCode < 300) {
                HttpEntity entity = response.getEntity();
                    if(entity != null) {
                        try {
                            JsonObject object = parser.parseObject(entity.getContent());
                            if(verbose) {
                                LOG.info("Request {}\nPAYLOAD<<<\n{}\n>>>\nRESPONSE {} <<<\n{}\n>>>", request.getRequestLine(),payload, statusCode, object);
                            }
                            return Optional.of(object);
                        } catch (JsonParseException e) {
                            if(verbose) {
                                LOG.info("Request {}\nPAYLOAD<<<\n{}\n>>>\nRESPONSE {} <<<\n{}\n>>>", request.getRequestLine(),payload, statusCode, "body unparseable");
                            }
                            throw new IllegalStateException("unparsable response entity");
                        }
                    } else {
                        throw new IllegalStateException("response has no entity");
                    }
            } else if(statusCode == 400) {
                JsonObject summary = getRequestResponseSummary(response);
                if(verbose) {
                    LOG.info("Request {}\nPAYLOAD<<<\n{}\n>>>\nSUMMARY {} <<<\n{}\n>>>", request.getRequestLine(),payload, statusCode, summary);
                }
                throw new EsBadRequestException(summary);
            } else if(statusCode == 404) {
                JsonObject summary = getRequestResponseSummary(response);
                if(verbose) {
                    LOG.info("Request {}\nPAYLOAD<<<\n{}\n>>>\nSUMMARY {} <<<\n{}\n>>>", request.getRequestLine(),payload, statusCode, summary);
                }
                return Optional.empty();
            } else if(statusCode == 409) {
                JsonObject summary = getRequestResponseSummary(response);
                if(verbose) {
                    LOG.info("Request {}\nPAYLOAD<<<\n{}\n>>>\nSUMMARY {} <<<\n{}\n>>>", request.getRequestLine(),payload, statusCode, summary);
                }
                throw new EsVersionConflictException(summary);
            } else if(statusCode == 429 || statusCode == 502 || statusCode == 503) {
                JsonObject summary = getRequestResponseSummary(response);
                throw new EsUnavailableException(summary);
            } else {
                JsonObject summary = getRequestResponseSummary(response);
                throw new IllegalStateException("unexpected http status " + statusCode + ": " + summary);
            }
        }

        private JsonObject getRequestResponseSummary(HttpResponse response) throws IOException {
            JsonObject summary = object(
                    field("uri", request.getURI().toString()),
                    field("method", request.getMethod()),
                    field("payload", payload)
                    );
            if(response.getEntity() != null) {
                String content = EntityUtils.toString(response.getEntity(), Charset.forName("utf-8"));
                try {
                    JsonObject object = parser.parseObject(content);
                    summary.put("response", object);
                } catch (JsonParseException e) {
                    summary.put("response", content);
                }
            }
            summary.removeEmpty();
            return summary;
        }
    }

    /**
     * Fix for the fact that httpclient HttpGet does not extend HttpEntityEnclosingRequestBase. Elasticsearch API includes gets with body so we need it.
     */
    private static class HttpGetWithBody extends HttpEntityEnclosingRequestBase {
        @Override
        public String getMethod() {
            return "GET";
        }

        public HttpGetWithBody(final String uri) {
            super();
            setURI(URI.create(uri));
        }
    }

    /**
     * Fix for the fact that httpclient HttpDelete does not extend HttpEntityEnclosingRequestBase. Elasticsearch API includes deletes with body so we need it.
     */
    private static class HttpDeleteWithBody extends HttpEntityEnclosingRequestBase {
        @Override
        public String getMethod() {
            return "DELETE";
        }

        public HttpDeleteWithBody(final String uri) {
            super();
            setURI(URI.create(uri));
        }
    }
}
