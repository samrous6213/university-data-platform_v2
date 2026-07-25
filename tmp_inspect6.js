var SimpleAWSCreds = Java.type("org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
var URI = Java.type("java.net.URI");
var Conf = Java.type("org.apache.hadoop.conf.Configuration");

// Create a test configuration with the access key and secret key
var conf = new Conf();
conf.set("fs.s3a.access.key", "testaccesskey");
conf.set("fs.s3a.secret.key", "testsecretkey");

// Create a test URI
var uri = new URI("s3a://test-bucket");

// Create the provider
var provider = new SimpleAWSCreds(uri, conf);

// Try to get credentials
try {
    var creds = provider.getCredentials();
    print("SUCCESS: AWSAccessKeyId=" + creds.getAWSAccessKeyId() + " AWSSecretKey=" + creds.getAWSSecretKey());
} catch (e) {
    print("FAILURE: " + e.message);
    print("Exception class: " + e.getClass().getName());
    print("Stack trace:");
    var sw = new java.io.StringWriter();
    var pw = new java.io.PrintWriter(sw);
    e.printStackTrace(pw);
    pw.flush();
    print(sw.toString());
}
