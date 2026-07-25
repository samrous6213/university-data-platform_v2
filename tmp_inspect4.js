var clazz = java.lang.Class.forName("org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
var ctors = clazz.getDeclaredConstructors();
for (var i = 0; i < ctors.length; i++) {
    print("Constructor: " + ctors[i]);
    var params = ctors[i].getParameterTypes();
    for (var j = 0; j < params.length; j++) {
        print("  param[" + j + "]: " + params[j].getName());
    }
}

var method = clazz.getDeclaredMethod("getCredentials");
print("\ngetCredentials bytecode:");
// We can't easily get bytecode, but let's check annotations

var method2 = clazz.getDeclaredMethod("getCredentials");
print("Return type: " + method2.getReturnType().getName());
