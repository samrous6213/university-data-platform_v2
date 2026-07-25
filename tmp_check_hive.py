import zipfile
z = zipfile.ZipFile('/opt/hive/lib/hive-exec-3.1.3.jar')
for n in z.namelist():
    if 'default' in n and n.endswith('.xml'):
        print(n)
        content = z.read(n).decode('utf-8')
        if 's3a.access.key' in content or 'fs.s3a' in content:
            for line in content.split('\n'):
                if 's3a' in line.lower():
                    print('  ', line.strip())
