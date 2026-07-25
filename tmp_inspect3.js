var clazz = java.lang.Class.forName("org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
var fields = clazz.getDeclaredFields();
for (var i = 0; i < fields.length; i++) {
    var f = fields[i];
    var mods = f.getModifiers();
    var isStatic = java.lang.reflect.Modifier.isStatic(mods);
    if (isStatic) {
        f.setAccessible(true);
        print(f.getName() + " = " + f.get(null));
    } else {
        print(f.getName() + " (instance)");
    }
}
