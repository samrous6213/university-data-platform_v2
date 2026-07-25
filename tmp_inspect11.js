var SimpleAWSCreds = Java.type("org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
var URI = Java.type("java.net.URI");
var Conf = Java.type("org.apache.hadoop.conf.Configuration");

// See which method the constructor uses by seeing if set vs credential provider works
var conf = new Conf();

// Option 1: Use conf.set (XML config style)
conf.set("fs.s3a.access.key", "from_set");
conf.set("fs.s3a.secret.key", "from_set");

var uri = new URI("s3a://test");
var provider = new SimpleAWSCreds(uri, conf);

try {
    var creds = provider.getCredentials();
    print("SET method works: accessKey=" + creds.getAWSAccessKeyId());
} catch (e) {
    print("SET method FAILED: " + e.message);
}

// Option 2: Use credential provider
// Actually, let me just test what getPassword returns
var conf2 = new Conf();
var passwd = conf2.getPassword("fs.s3a.access.key");
if (passwd) {
    print("getPassword returned: " + new java.lang.String(passwd));
} else {
    print("getPassword returned null");
}
