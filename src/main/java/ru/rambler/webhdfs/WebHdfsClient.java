package ru.rambler.webhdfs;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WebHdfsClient {
    URI[] hosts;
    String username;
    int timeout;
    int numRetries;

    HttpClient client;
    final Logger logger = LoggerFactory.getLogger(WebHdfsClient.class);

    public static int DEFAULT_TIMEOUT = 60;
    public static int DEFAULT_RETRIES = 3;
    public static String API_PATH = "/webhdfs/v1/";

    public WebHdfsClient(URI[] hosts, String username, int timeout, int retries, HttpClient client) {
        this.hosts = hosts;
        this.username = username;
        this.timeout = timeout;
        this.numRetries = retries;
        this.client = client;
    }

    public static WebHdfsClient initiate(URI[] hosts, String username) {
        HttpClientBuilder builder = HttpClientBuilder.create();
        builder.setConnectionManager(new PoolingHttpClientConnectionManager());
        builder.setConnectionManagerShared(true);
        return new WebHdfsClient(hosts, username, DEFAULT_TIMEOUT, DEFAULT_RETRIES, builder.build());
    }

    public static class WebHdfsException extends RuntimeException {
        public WebHdfsException() {
            super();
        }

        public WebHdfsException(String message) {
            super(message);
        }

        public WebHdfsException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static class RemoteException extends WebHdfsException {
        public RemoteException(String message) {
            super(message);
        }
    }

    public static class NetworkError extends WebHdfsException {
        public NetworkError(Throwable cause) {
            super("Network error", cause);
        }
    }

    public static class ActiveHostNotFound extends WebHdfsException {
        public ActiveHostNotFound(String message, Exception exc) {
            super(message, exc);
        }
    }

    public static class NotFound extends WebHdfsException {
        public NotFound(String message) {
            super(message);
        }
    }

    public static class AlreadyExists extends WebHdfsException {
        public AlreadyExists(String message) {
            super(message);
        }
    }

    public static class BadRequest extends WebHdfsException {
        public BadRequest(String message) {
            super(message);
        }
    }

    public static class Unauthorized extends WebHdfsException {
        public Unauthorized(String message) {
            super(message);
        }
    }

    public static class Forbidden extends WebHdfsException {
        public Forbidden(String message) {
            super(message);
        }
    }

    public static class ServerError extends WebHdfsException {
        public ServerError(String message) {
            super(message);
        }
    }

    public static class StandbyException extends WebHdfsException {
        public StandbyException() {
            super();
        }
    }

    public URI[] getHosts() {
        return hosts;
    }

    public String getUsername() {
        return username;
    }

    protected HttpResponse request(HttpUriRequest request) {
        logger.debug("HTTP [{}] '{}'", request.getMethod(), request.getURI());
        try {
            HttpResponse response = this.client.execute(request);
            int code = response.getStatusLine().getStatusCode();
            logger.debug("CODE [{}]", code);

            if (code >= 400) {
                String message = EntityUtils.toString(response.getEntity());
                JSONObject json = null;
                try {
                    json = new JSONObject(message);
                    message = json.getJSONObject("RemoteException").toString();
                } catch (JSONException e) {
                }

                if (code == 400) {
                    throw new BadRequest(message);
                } else if (code == 401) {
                    throw new Unauthorized(message);
                } else if (code == 403) {
                    if (json != null) {
                        try {
                            String exc = json.getJSONObject("RemoteException").getString("exception");
                            if (exc.equals("StandbyException")) {
                                throw new StandbyException();
                            } else if (exc.equals("FileAlreadyExistsException")) {
                                throw new AlreadyExists(message);
                            }
                        } catch (JSONException e) {
                        }
                    }
                    throw new Forbidden(message);
                } else if (code == 404) {
                    throw new NotFound(message);
                } else if (code == 500) {
                    if (json != null) {
                        try {
                            String exc = json.getJSONObject("RemoteException").getString("exception");
                            if (exc.equals("FileAlreadyExistsException")) {
                                throw new AlreadyExists(message);
                            }
                        } catch (JSONException e) {
                        }
                    }
                    throw new ServerError(message);
                } else {
                    throw new RemoteException(
                        String.format("HTTP code: %s\n Message: %s", Integer.toString(code), message)
                    );
                }
            }

            return response;
        } catch (IOException exc) {
            throw new NetworkError(exc);
        }
    }

    protected URI buildURI(
        URI host, String pathStr, String operation, List<NameValuePair> params
    ) throws URISyntaxException {
        URIBuilder uriBuilder = new URIBuilder(host).setPath(API_PATH + pathStr).addParameter("op", operation)
            .addParameter("user.name", this.getUsername());
        if (params != null) {
            uriBuilder.addParameters(params);
        }
        return uriBuilder.build();
    }

    protected HttpResponse requestAny(String method, Path path, String operation, List<NameValuePair> params) {
        String pathStr = path.toAbsolutePath().toString().replaceAll("^/", "");

        Exception lastException = null;
        for (URI host : this.getHosts()) {
            try {
                URI requestURI = buildURI(host, pathStr, operation, params);
                HttpUriRequest request = RequestBuilder.create(method).setUri(requestURI).build();
                return this.request(request);
            } catch (NetworkError e) {
                logger.info(String.format("Host '%s' is inactive, or unreachable", host));
                lastException = e;
                continue;
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Invalid assembled URI", e);
            }
        }

        throw new ActiveHostNotFound("Not found active host", lastException);
    }

    protected JSONObject requestAnyJson(String method, Path path, String operation, List<NameValuePair> params) {
        try {
            return new JSONObject(EntityUtils.toString(this.requestAny(method, path, operation, params).getEntity()));
        } catch (IOException e) {
            throw new NetworkError(e);
        }
    }

    public WebHdfsResource resource(Path path) {
        return new WebHdfsResource(this, path);
    }

    public WebHdfsResource resource(String path) {
        return new WebHdfsResource(this, Paths.get(path));
    }
}
