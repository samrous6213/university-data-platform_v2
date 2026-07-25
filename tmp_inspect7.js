var SimpleAWSCreds = Java.type("org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
var URI = Java.type("java.net.URI");
var Conf = Java.type("org.apache.hadoop.conf.Configuration");

// Test 1: Check if getCredentials reads from config at call time
var conf = new Conf();
conf.set("fs.s3a.access.key", "before");
conf.set("fs.s3a.secret.key", "before");

var uri = new URI("s3a://test-bucket");
var provider = new SimpleAWSCreds(uri, conf);

// Change config AFTER creating provider
conf.set("fs.s3a.access.key", "after");
conf.set("fs.s3a.secret.key", "after");

var creds = provider.getCredentials();
print("Test 1 - After changing config: accessKey=" + creds.getAWSAccessKeyId() + " secretKey=" + creds.getAWSSecretKey());

// Test 2: Check if removing the values from config causes failure
conf.unset("fs.s3a.access.key");
try {
    creds = provider.getCredentials();
    print("Test 2 - After unsetting access key: Still worked! accessKey=" + creds.getAWSAccessKeyId());
} catch (e) {
    print("Test 2 - After unsetting: Failed as expected: " + e.message);
}
