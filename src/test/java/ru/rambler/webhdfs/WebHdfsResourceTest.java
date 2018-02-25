package ru.rambler.webhdfs;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.ITestResult;
import org.testng.annotations.*;
import org.testng.util.RetryAnalyzerCount;


public class WebHdfsResourceTest extends Assert {
    public class NetworkErrorRetryAnalyzerCount extends RetryAnalyzerCount {
        AtomicInteger count = new AtomicInteger(3);
        final Logger logger = LoggerFactory.getLogger(NetworkErrorRetryAnalyzerCount.class);

        @Override
        public boolean retryMethod(ITestResult result) {
            Throwable exception = result.getThrowable();
            logger.error("Got exception:", exception);
            return (exception != null && WebHdfsClient.NetworkError.class.isInstance(exception));
        }
    };

    private WebHdfsClient client;
    private WebHdfsResource testsRoot;
    private WebHdfsResource testResource;

    @BeforeClass
    void init() {
        this.client = WebHdfsClientTest.makeClient();
        this.testsRoot = this.client.resource(Paths.get(WebHdfsClientTest.getTestRoot()));
        this.testsRoot.mkdir(true);
    }

    @AfterClass
    void teardown() {
        this.testsRoot.remove(true);
    }

    @BeforeMethod
    void makeTestDir() {
        this.testResource = this.testsRoot.child(UUID.randomUUID().toString());
    }

    @AfterMethod
    void dropTestDir() {
        this.testResource.remove(true);
    }

    @Test
    void testLs() {
        testResource.mkdir(true);
        WebHdfsResource child = testResource.child("hello");
        child.child("moo").child("meow").mkdir(true);
        if (!child.child("file").exists()) {
            child.child("file").create("Hello");
        }

        boolean fileExists = false;
        boolean dirExists = false;

        for (Iterator<WebHdfsResource> iter = testResource.lsResources(true); iter.hasNext();) {
            WebHdfsResource res = iter.next();
            if (res.getBaseName().equals("file") && res.isFile() && !res.isDir()) {
                fileExists = true;
            }
            if (res.getBaseName().equals("meow") && res.isDir() && !res.isFile()) {
                dirExists = true;
            }
        }
        assertTrue(fileExists);
        assertTrue(dirExists);

        fileExists = false;
        dirExists = false;

        for (Iterator<WebHdfsResource> iter = testResource.lsResources(false); iter.hasNext();) {
            WebHdfsResource res = iter.next();
            if (res.getBaseName().equals("file") && res.isFile() && !res.isDir()) {
                fileExists = true;
            }
            if (res.getBaseName().equals("hello") && res.isDir() && !res.isFile()) {
                dirExists = true;
            }
        }
        assertFalse(fileExists);
        assertTrue(dirExists);
    }

    InputStream makeBoundedRandomStream(long seed, long size) {
        return new BoundedInputStream(new RandomInputStream(seed), size);
    }

    private static String getMD5Checksum(InputStream in) throws IOException {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] byteArray = new byte[1024];
            int bytesCount = 0;

            while ((bytesCount = in.read(byteArray)) != -1) {
                md.update(byteArray, 0, bytesCount);
            }
            in.close();

            byte[] bytes = md.digest();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < bytes.length; i++) {
                sb.append(Integer.toString((bytes[i] & 0xff) + 0x100, 16).substring(1));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException exc) {
            throw new RuntimeException(exc);
        }
    }

    @DataProvider
    Object[][] uploadConfs() {
        return new Object[][] { { 0L, 2048L }, { 8L, 20000000L }, // ~20Mb
            { 58L, 500000000L }, // ~500Mb
        };
    }

    @Test(dataProvider = "uploadConfs", retryAnalyzer = NetworkErrorRetryAnalyzerCount.class)
    void testUploadDownloadStream(long seed, long size) throws IOException {
        InputStream stream = makeBoundedRandomStream(seed, size);
        String md5 = getMD5Checksum(stream);
        stream = makeBoundedRandomStream(seed, size);

        for (int n_try = 3; n_try != 0; n_try--) {
            try {
                WebHdfsResource fileResource = this.testResource.child("uploaded");
                fileResource.create(stream);
                InputStream downloadStream = fileResource.open();
                String downloadMD5 = getMD5Checksum(downloadStream);
                assertEquals(md5, downloadMD5);
                break;
            } catch (WebHdfsClient.NetworkError error) {
            }
        }
    }

    @Test
    void testGetSetAttributes() {
        WebHdfsResource file = testResource.child("file");
        long blockSize = 1L << 27; // 128 MiB
        short replication = 4;
        String permission = "777";
        String text = "Hello";
        file.create(new ByteArrayInputStream(text.getBytes()), true, blockSize, replication, permission, 4096L);

        assertEquals(file.getPermission(), permission);
        assertEquals(file.getReplication(), replication);
        assertEquals(file.getBlockSize(), blockSize);

        long childrenNum = testResource.getChildrenNum();
        assertEquals(childrenNum, 1L);
        String owner = file.getOwner();
        String group = file.getGroup();
        long size = file.getLength();
        assertEquals(text.getBytes().length, size);

        Path path = file.getPath();
        file.rename(Paths.get(path.getParent().toString(), "moved"));
        file = client.resource(file.getPath());
        assertFalse(file.exists());

        WebHdfsResource moved = testResource.child("moved");
        assertEquals(moved.getPermission(), permission);
        assertEquals(moved.getReplication(), replication);
        assertEquals(moved.getBlockSize(), blockSize);
        assertEquals(owner, moved.getOwner());
        assertEquals(group, moved.getGroup());
        assertEquals(size, moved.getLength());

        assertEquals(childrenNum, testResource.getChildrenNum());

    }

    @Test(retryAnalyzer = NetworkErrorRetryAnalyzerCount.class)
    void testAppend() throws IOException, InterruptedException {
        WebHdfsResource file = testResource.child("file");
        file.touch();
        Thread.sleep(1000L);
        file.append("Hello");
        byte[] second = " World".getBytes();
        file.append(new ByteArrayInputStream(second), (long) second.length);
        String readed = IOUtils.toString(file.open(0L, 1024L, 1024L), Charset.defaultCharset());
        assertEquals(readed, "Hello World");
    }
}
