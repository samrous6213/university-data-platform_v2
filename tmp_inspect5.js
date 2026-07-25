var clazz = java.lang.Class.forName("org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");

// look at all fields including inherited
var allFields = clazz.getFields();
for (var i = 0; i < allFields.length; i++) {
    var f = allFields[i];
    var mods = f.getModifiers();
    if (java.lang.reflect.Modifier.isStatic(mods)) {
        print("Field: " + f.getName() + " = " + f.get(null));
    }
}

// check interfaces
var ifaces = clazz.getInterfaces();
for (var i = 0; i < ifaces.length; i++) {
    print("Interface: " + ifaces[i].getName());
    var ifFields = ifaces[i].getFields();
    for (var j = 0; j < ifFields.length; j++) {
        print("  Field: " + ifFields[j].getName() + " = " + ifFields[j].get(null));
    }
}

// check parent class
var parent = clazz.getSuperclass();
print("Superclass: " + (parent ? parent.getName() : "none"));
