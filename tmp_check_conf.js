var Conf = Java.type("org.apache.hadoop.conf.Configuration");
var conf = new Conf();
var accessKey = conf.get("fs.s3a.access.key");
var secretKey = conf.get("fs.s3a.secret.key");
var endpoint = conf.get("fs.s3a.endpoint");
var provider = conf.get("fs.s3a.aws.credentials.provider");

print("fs.s3a.access.key = '" + (accessKey != null ? accessKey : "null") + "'");
print("fs.s3a.secret.key = '" + (secretKey != null ? secretKey : "null") + "'");
print("fs.s3a.endpoint = '" + (endpoint != null ? endpoint : "null") + "'");
print("fs.s3a.aws.credentials.provider = '" + (provider != null ? provider : "null") + "'");
