var URL = Java.type("java.net.URL");
var URLClassLoader = Java.type("java.net.URLClassLoader");
var File = Java.type("java.io.File");

var jar = new File("/opt/hive/auxlib/hadoop-aws-3.1.0.jar");
var url = jar.toURI().toURL();
var cl = new URLClassLoader([url]);

var scanPkg = function(pkg) {
    try {
        var cls = cl.loadClass(pkg);
        print(cls.getName());
    } catch (e) {
        // error
    }
};

// Try loading known AWS credential providers
var candidates = [
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "org.apache.hadoop.fs.s3a.EnvironmentVariableCredentialsProvider",
    "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",  
    "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
    "org.apache.hadoop.fs.s3a.SharedInstanceProfileCredentialsProvider",
    "org.apache.hadoop.fs.s3a.InstanceProfileCredentialsProvider",
    "org.apache.hadoop.fs.s3a.AssumeRoleAWSCredentialsProvider",
    "org.apache.hadoop.fs.s3a.auth.AssumeRoleAWSCredentialsProvider",
    "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
    "org.apache.hadoop.fs.s3a.auth.delegation.DelegationTokenProvider"
];

for (var i = 0; i < candidates.length; i++) {
    try {
        var cls = Java.type(candidates[i]);
        print("FOUND: " + candidates[i] + " implements: " + cls.interfaces);
    } catch (e) {
        print("MISS: " + candidates[i] + " - " + e.message.substring(0, 60));
    }
}
