var SimpleAWSCreds = Java.type("org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
var URI = Java.type("java.net.URI");
var Conf = Java.type("org.apache.hadoop.conf.Configuration");

// Test: Simulate what S3AFileSystem.initialize() does
var originalConf = new Conf();
originalConf.set("fs.s3a.access.key", "minioadmin");
originalConf.set("fs.s3a.secret.key", "minioadmin");
originalConf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");

// Clone the configuration (like S3AFileSystem does)
var clonedConf = new Conf(originalConf);

// Check if clone has the properties
print("clone fs.s3a.access.key = [" + clonedConf.get("fs.s3a.access.key") + "]");
print("clone fs.s3a.secret.key = [" + clonedConf.get("fs.s3a.secret.key") + "]");

// Create provider with cloned config
var uri = new URI("s3a://test-bucket");
var provider = new SimpleAWSCreds(uri, clonedConf);

try {
    var creds = provider.getCredentials();
    print("SUCCESS: accessKey=" + creds.getAWSAccessKeyId());
} catch (e) {
    print("FAILED: " + e.message);
}
