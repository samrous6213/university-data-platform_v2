var Conf = Java.type("org.apache.hadoop.conf.Configuration");
var HiveConf = Java.type("org.apache.hadoop.hive.conf.HiveConf");

// Create a HiveConf (this loads hive-site.xml from classpath)
var hiveConf = new HiveConf();

// Check if HiveConf has the S3A properties
print("HiveConf fs.s3a.access.key = [" + hiveConf.get("fs.s3a.access.key") + "]");
print("HiveConf fs.s3a.secret.key = [" + hiveConf.get("fs.s3a.secret.key") + "]");
print("HiveConf fs.s3a.endpoint = [" + hiveConf.get("fs.s3a.endpoint") + "]");
print("HiveConf hive.metastore.uris = [" + hiveConf.get("hive.metastore.uris") + "]");
print("HiveConf fs.defaultFS = [" + hiveConf.get("fs.defaultFS") + "]");

// Now check what Configuration (non-Hive) loads
var plainConf = new Conf();
print("\nPlain Configuration:");
print("  fs.s3a.access.key = [" + plainConf.get("fs.s3a.access.key") + "]");
print("  fs.s3a.secret.key = [" + plainConf.get("fs.s3a.secret.key") + "]");
print("  fs.s3a.endpoint = [" + plainConf.get("fs.s3a.endpoint") + "]");
print("  fs.defaultFS = [" + plainConf.get("fs.defaultFS") + "]");
