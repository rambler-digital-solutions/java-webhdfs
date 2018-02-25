package ru.rambler.webhdfs;


import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicNameValuePair;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WebHdfsResource {
    private WebHdfsClient client;
    private Path path;
    private boolean isExtented;

    private long accessTime;
    private long blockSize;
    private long childrenNum;
    private long fileId;
    private String group;
    private long length;
    private long modificationTime;
    private String owner;
    private String permission;
    private short replication;
    private short storagePolicy;
    private boolean isdir;

    final Logger logger = LoggerFactory.getLogger(WebHdfsResource.class);

    public WebHdfsResource(WebHdfsClient client, Path path) {
        this.client = client;
        this.path = path;
        this.isExtented = false;
    }

    @Override
    public String toString() {
        return "WebHdfsResource{" + "path=" + path.toAbsolutePath().toString() + '}';
    }

    void fillFileStatus(JSONObject stat) {
        this.isExtented = true;
        this.accessTime = stat.getLong("accessTime");
        this.modificationTime = stat.getLong("modificationTime");
        this.fileId = stat.getLong("fileId");
        this.length = stat.getLong("length");
        this.blockSize = stat.getLong("blockSize");
        this.childrenNum = stat.getLong("childrenNum");
        this.owner = stat.getString("owner");
        this.group = stat.getString("group");
        this.isdir = stat.getString("type").equals("DIRECTORY");
        this.permission = stat.getString("permission");
        this.replication = (short) stat.getInt("replication");
        this.storagePolicy = (short) stat.getInt("storagePolicy");
    }

    public WebHdfsResource child(Path path) {
        return new WebHdfsResource(this.client, this.path.resolve(path));
    }

    public WebHdfsResource child(String path) {
        return child(Paths.get(path));
    }

    WebHdfsResource child(JSONObject stat) {
        WebHdfsResource child = new WebHdfsResource(this.client, this.path.resolve(stat.getString("pathSuffix")));
        child.fillFileStatus(stat);
        return child;
    }

    public WebHdfsResource parent() {
        return new WebHdfsResource(this.client, this.path.getParent());
    }

    JSONObject listStatus() {
        return this.client.requestAnyJson("GET", this.path, "LISTSTATUS", null);
    }

    public static class ParamsBuilder {
        private List<NameValuePair> params = new ArrayList<NameValuePair>();

        public static ParamsBuilder create() {
            return new ParamsBuilder();
        }

        public ParamsBuilder add(String name, Object value) {
            if (value != null) {
                params.add(new BasicNameValuePair(name, value.toString()));
            }
            return this;
        }

        public List<NameValuePair> build() {
            return params;
        }
    }

    public class ResourceIterator implements Iterator<WebHdfsResource> {
        WebHdfsResource root;
        WebHdfsResource next;
        Iterator<Object> selfIter;
        Iterator<WebHdfsResource> childIter;
        boolean recursive;

        public ResourceIterator(WebHdfsResource resource, boolean recursive) {
            this.recursive = recursive;
            this.root = resource;

            JSONArray statuses = root.listStatus().getJSONObject("FileStatuses").getJSONArray("FileStatus");
            if (statuses.length() > 0) {
                selfIter = statuses.iterator();
                next = root.child((JSONObject) selfIter.next());
                childIter = getChildIter(next);
            } else {
                next = null;
            }
        }

        ResourceIterator getChildIter(WebHdfsResource resource) {
            if (resource.isDir() && this.recursive) {
                return new ResourceIterator(resource, this.recursive);
            } else {
                return null;
            }
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public WebHdfsResource next() {
            WebHdfsResource toReturn = next;

            if (childIter != null && childIter.hasNext()) {
                next = childIter.next();
            } else {
                if (selfIter.hasNext()) {
                    next = root.child((JSONObject) selfIter.next());
                    childIter = getChildIter(next);
                } else {
                    next = null;
                }
            }

            return toReturn;
        }
    }

    public Iterator<WebHdfsResource> lsResources(boolean recursive) {
        return new ResourceIterator(this, recursive);
    }

    public JSONObject getFileStatus() {
        return client.requestAnyJson("GET", this.path, "GETFILESTATUS", null).getJSONObject("FileStatus");
    }

    public void extendStat() {
        if (!isExtented) {
            this.fillFileStatus(getFileStatus());
        }
    }

    void createInner(AbstractHttpEntity entity, List<NameValuePair> params) {
        HttpResponse response = client.requestAny("PUT", this.path, "CREATE", params);
        String location = response.getFirstHeader("location").getValue();
        client.request(RequestBuilder.create("PUT").setUri(location).setEntity(entity).build());
    }

    public void create(
        InputStream data, Boolean overwrite, Long blockSize, Short replication, String permission, Long bufferSize
    ) {
        ParamsBuilder paramsBuilder = ParamsBuilder.create();
        paramsBuilder.add("overwrite", overwrite);
        paramsBuilder.add("blockSize", blockSize);
        paramsBuilder.add("replication", replication);
        paramsBuilder.add("permission", permission);
        paramsBuilder.add("buffersize", bufferSize);

        InputStreamEntity entity = new InputStreamEntity(data, ContentType.APPLICATION_OCTET_STREAM);
        createInner(entity, paramsBuilder.build());
    }

    public void create(String data) {
        StringEntity entity = new StringEntity(data, ContentType.APPLICATION_OCTET_STREAM);
        createInner(entity, null);
    }

    public void create(InputStream data) {
        InputStreamEntity entity = new InputStreamEntity(data, ContentType.APPLICATION_OCTET_STREAM);
        createInner(entity, null);
    }

    public void touch() {
        create("");
    }

    void appendInner(AbstractHttpEntity entity, List<NameValuePair> params) {
        HttpResponse response = client.requestAny("POST", this.path, "APPEND", params);
        String location = response.getFirstHeader("location").getValue();
        client.request(RequestBuilder.create("POST").setUri(location).setEntity(entity).build());
    }

    public void append(String data) {
        StringEntity entity = new StringEntity(data, ContentType.APPLICATION_OCTET_STREAM);
        appendInner(entity, null);
    }

    public void append(InputStream data, Long bufferSize) {
        InputStreamEntity entity = new InputStreamEntity(data, ContentType.APPLICATION_OCTET_STREAM);
        appendInner(entity, ParamsBuilder.create().add("buffersize", bufferSize).build());
    }

    InputStream openInner(List<NameValuePair> params) {
        try {
            HttpResponse response = client.requestAny("GET", this.path, "OPEN", params);
            return response.getEntity().getContent();
        } catch (IOException e) {
            throw new WebHdfsClient.NetworkError(e);
        }
    }

    public InputStream open() {
        return openInner(null);
    }

    public InputStream open(Long offset, Long length, Long bufferSize) {
        ParamsBuilder paramsBuilder = ParamsBuilder.create();
        paramsBuilder.add("offset", offset);
        paramsBuilder.add("length", length);
        paramsBuilder.add("buffersize", bufferSize);
        return openInner(paramsBuilder.build());
    }

    public boolean rename(String destination) {
        List<NameValuePair> params = ParamsBuilder.create().add("destination", destination).build();
        return client.requestAnyJson("PUT", this.path, "RENAME", params).getBoolean("boolean");
    }

    public boolean rename(Path destination) {
        return rename(destination.toString());
    }

    public boolean remove(boolean recursive) {
        List<NameValuePair> params = ParamsBuilder.create().add("recursive", new Boolean(recursive)).build();
        return client.requestAnyJson("DELETE", this.path, "DELETE", params).getBoolean("boolean");
    }

    public boolean exists() {
        try {
            extendStat();
            return true;
        } catch (WebHdfsClient.NotFound e) {
            return false;
        }
    }

    boolean mkdirInner(String permission) {
        List<NameValuePair> params = ParamsBuilder.create().add("permission", permission).build();
        return client.requestAnyJson("PUT", this.path, "MKDIRS", params).getBoolean("boolean");
    }

    public boolean mkdir(boolean parents) {
        return mkdir(parents, null);
    }

    public boolean mkdir(boolean parents, String permission) {
        if (parents) {
            if (!this.parent().exists()) {
                this.parent().mkdir(parents, permission);
            }
        }
        return mkdirInner(permission);
    }

    public boolean isDir() {
        extendStat();
        return isdir;
    }

    public boolean isFile() {
        extendStat();
        return !isdir;
    }

    public String getBaseName() {
        return path.getFileName().toString();
    }

    public Path getPath() {
        return path;
    }

    public long getAccessTime() {
        extendStat();
        return accessTime;
    }

    public long getBlockSize() {
        extendStat();
        return blockSize;
    }

    public long getChildrenNum() {
        extendStat();
        return childrenNum;
    }

    public long getFileId() {
        extendStat();
        return fileId;
    }

    public String getGroup() {
        extendStat();
        return group;
    }

    public long getLength() {
        extendStat();
        return length;
    }

    public long getModificationTime() {
        extendStat();
        return modificationTime;
    }

    public String getOwner() {
        extendStat();
        return owner;
    }

    public String getPermission() {
        extendStat();
        return permission;
    }

    public short getReplication() {
        extendStat();
        return replication;
    }

    public short getStoragePolicy() {
        extendStat();
        return storagePolicy;
    }
}
