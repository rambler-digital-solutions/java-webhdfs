package ru.rambler.webhdfs;


import static org.mockito.Mockito.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.http.*;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class WebHdfsClientTest extends Assert {
    @Mock
    HttpClient httpClientMock;
    @Mock
    HttpResponse httpResponse;
    @Mock
    StatusLine statusLineMock;
    HttpEntity httpEntity;
    WebHdfsClient realClient;
    WebHdfsClient mockedClient;

    static URI[] getTestsURIs() {
        String urls = System.getProperty("webhdfs.urls");
        if (urls == null) {
            throw new IllegalArgumentException("For run integration tests please specify \"webhdfs.urls\" property");
        }

        try {
            String[] urlsArr = urls.split(",");
            URI[] hosts = new URI[urlsArr.length];
            for (int i = 0; i < urlsArr.length; i++) {
                hosts[i] = new URI(urlsArr[i]);
            }

            return hosts;
        } catch (URISyntaxException e) {
            throw new RuntimeException();
        }
    }

    static WebHdfsClient makeClient() {
        return WebHdfsClient.initiate(getTestsURIs(), getTestUser());
    }

    static String getTestRoot() {
        String testRoot = System.getProperty("webhdfs.test.root");
        if (testRoot == null) {
            throw new IllegalArgumentException(
                "For run integration tests please specify \"webhdfs.test.root\" property"
            );
        }
        return testRoot;
    }

    static String getTestUser() {
        String testRoot = System.getProperty("webhdfs.test.user");
        if (testRoot == null) {
            throw new IllegalArgumentException(
                "For run integration tests please specify \"webhdfs.test.user\" property"
            );
        }
        return testRoot;
    }

    @BeforeMethod
    public void init() throws IOException {
        MockitoAnnotations.initMocks(this);
        when(httpClientMock.execute(any(HttpUriRequest.class))).thenReturn(httpResponse);
        when(httpResponse.getStatusLine()).thenReturn(statusLineMock);
        this.httpEntity = new StringEntity("{}", ContentType.APPLICATION_JSON);
        when(httpResponse.getEntity()).thenReturn(httpEntity);
        mockedClient = new WebHdfsClient(getTestsURIs(), "", 0, 0, httpClientMock);
        realClient = makeClient();
    }

    @DataProvider
    Object[][] exitCodesToExc() {
        return new Object[][] { { 400, "{}", WebHdfsClient.BadRequest.class },
            { 401, "{}", WebHdfsClient.Unauthorized.class }, { 403, "{}", WebHdfsClient.Forbidden.class },
            { 403, "not a json", WebHdfsClient.Forbidden.class },
            { 403, "{\"RemoteException\": {\"exception\": \"StandbyException\"}}",
                WebHdfsClient.StandbyException.class },
            { 403, "{\"RemoteException\": {\"exception\": \"FileAlreadyExistsException\"}}",
                WebHdfsClient.AlreadyExists.class },
            { 403, "{\"RemoteException\": {\"exception\": \"OtherException\"}}", WebHdfsClient.Forbidden.class },
            { 404, "{}", WebHdfsClient.NotFound.class }, { 500, "{}", WebHdfsClient.ServerError.class },
            { 500, "not a json", WebHdfsClient.ServerError.class },
            { 500, "{\"RemoteException\": {\"exception\": \"FileAlreadyExistsException\"}}",
                WebHdfsClient.AlreadyExists.class },
            { 500, "{\"RemoteException\": {\"exception\": \"OtherException\"}}", WebHdfsClient.ServerError.class },
            { 501, "{}", WebHdfsClient.RemoteException.class }, };
    }

    @Test(dataProvider = "exitCodesToExc")
    void testHandlingRequestStatusCodes(int statusCode, String response, Class<?> excClass) {
        WebHdfsResource resource = mockedClient.resource(getTestRoot());
        this.httpEntity = new StringEntity(response, ContentType.APPLICATION_JSON);
        when(httpResponse.getEntity()).thenReturn(httpEntity);
        when(statusLineMock.getStatusCode()).thenReturn(statusCode);

        try {
            resource.mkdir(false);
            fail();
        } catch (Exception exc) {
            assertTrue(excClass.isInstance(exc));
        }
    }

    @Test(expectedExceptions = WebHdfsClient.AlreadyExists.class)
    void testStandbyThrows() {
        this.httpEntity = new StringEntity(
            "{\"RemoteException\": {\"exception\": \"FileAlreadyExistsException\"}}", ContentType.APPLICATION_JSON
        );
        when(httpResponse.getEntity()).thenReturn(httpEntity);
        when(statusLineMock.getStatusCode()).thenReturn(403);
        WebHdfsResource resource = mockedClient.resource(getTestRoot());
        resource.isDir();
    }

    @Test(expectedExceptions = WebHdfsClient.ActiveHostNotFound.class)
    void testNotFoundActiveHost() {
        WebHdfsClient stubbed = new WebHdfsClient(getTestsURIs(), "", 0, 0, httpClientMock) {
            @Override
            protected HttpResponse request(HttpUriRequest request) {
                throw new NetworkError(new RuntimeException());
            }
        };
        stubbed.resource(getTestRoot()).isDir();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    void testIllegalBuildedURI() {
        WebHdfsClient stubbed = new WebHdfsClient(getTestsURIs(), "", 0, 0, httpClientMock) {
            @Override
            protected URI buildURI(
                URI host, String pathStr, String operation, List<NameValuePair> params
            ) throws URISyntaxException {
                throw new URISyntaxException("", "");
            }
        };
        stubbed.resource(getTestRoot()).isDir();
    }

    @Test(expectedExceptions = WebHdfsClient.NetworkError.class)
    void testReThrowNetworkErrorOnLoadJsonResponse() {
        when(statusLineMock.getStatusCode()).thenReturn(200);
        when(httpResponse.getEntity()).thenReturn(new StringEntity("", ContentType.APPLICATION_JSON) {
            @Override
            public InputStream getContent() throws IOException {
                throw new IOException();
            }
        });
        this.mockedClient.resource(getTestRoot()).isDir();
    }

    @Test(expectedExceptions = WebHdfsClient.NetworkError.class)
    void testReThrowNetworkErrorOnRequest() throws IOException {
        when(statusLineMock.getStatusCode()).thenReturn(200);
        when(httpClientMock.execute(any(HttpUriRequest.class))).thenThrow(new IOException());
        this.mockedClient.request(mock(HttpUriRequest.class));
    }
}
