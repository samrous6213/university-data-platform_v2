var Conf = Java.type("org.apache.hadoop.conf.Configuration");
var conf = new Conf();
var a = conf.get("fs.s3a.access.key");
var s = conf.get("fs.s3a.secret.key");
var e = conf.get("fs.s3a.endpoint");
var p = conf.get("fs.s3a.aws.credentials.provider");

if (a === null) { a = "null"; }
if (s === null) { s = "null"; }
if (e === null) { e = "null"; }
if (p === null) { p = "null"; }

print("fs.s3a.access.key=[" + a + "]");
print("fs.s3a.secret.key=[" + s + "]");
print("fs.s3a.endpoint=[" + e + "]");
print("provider=[" + p + "]");

var HiveConf = Java.type("org.apache.hadoop.hive.conf.HiveConf");
var hive = new HiveConf();
var ha = hive.get("fs.s3a.access.key");
var hs = hive.get("fs.s3a.secret.key");
var he = hive.get("fs.s3a.endpoint");
var mu = hive.get("hive.metastore.uris");

if (ha === null) { ha = "null"; }
if (hs === null) { hs = "null"; }
if (he === null) { he = "null"; }
if (mu === null) { mu = "null"; }

print("Hive: fs.s3a.access.key=[" + ha + "]");
print("Hive: fs.s3a.secret.key=[" + hs + "]");
print("Hive: fs.s3a.endpoint=[" + he + "]");
print("Hive: hive.metastore.uris=[" + mu + "]");
