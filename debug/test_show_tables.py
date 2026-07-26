from src.transformations.spark.config.spark_session import get_spark_session
# adapte le nom de la fonction si elle s'appelle autrement dans ton fichier

spark = get_spark_session("check_hive_tables")

print("=== SHOW TABLES ===")
spark.sql("SHOW TABLES").show(truncate=False)

print("=== faculty_profiles ===")
spark.sql("SELECT COUNT(*) AS total FROM faculty_profiles").show()

print("=== course_catalog ===")
spark.sql("SELECT COUNT(*) AS total FROM course_catalog").show()

spark.stop()