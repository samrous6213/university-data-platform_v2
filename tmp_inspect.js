var clazz = Java.type("org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
var methods = clazz.getDeclaredMethods();
for (var i = 0; i < methods.length; i++) {
    var m = methods[i];
    print(m.getName() + " -> " + m);
}
print("---");
var fields = clazz.getDeclaredFields();
for (var i = 0; i < fields.length; i++) {
    var f = fields[i];
    print(f.getName() + " = " + f.get(null));
}
