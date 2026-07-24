"""
Module d'écriture générique vers Apache Hudi (Silver Zone).
Garantit l'idempotence via des opérations d'upsert.
"""
from pyspark.sql import DataFrame

def write_to_hudi(df: DataFrame, table_name: str, primary_key: str, precombine_field: str, base_path: str):
    """
    Sauvegarde un DataFrame valide au format Hudi avec la méthode d'upsert.
    """
    # --- AJOUT DU BOUCLIER ---
    if df.count() == 0:
        print(f"⚠️ Aucune donnée valide trouvée pour '{table_name}'. Écriture Hudi ignorée.")
        return
    # -------------------------

    hudi_options = {
        # Nom de la table Hudi
        "hoodie.table.name": table_name,
        
        # Clé primaire (pour savoir quelle ligne mettre à jour)
        "hoodie.datasource.write.recordkey.field": primary_key,
        
        # Champ de pré-combinaison (souvent un timestamp) pour garder la donnée la plus récente en cas de conflit
        "hoodie.datasource.write.precombine.field": precombine_field,
        
        # Opération Upsert
        "hoodie.datasource.write.operation": "upsert",
        
        # Optimisation pour un environnement local (MVP) pour éviter de saturer la RAM
        "hoodie.upsert.shuffle.parallelism": 2,
        "hoodie.insert.shuffle.parallelism": 2,

       # --- SYNCHRONISATION HIVE METASTORE ---
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.mode": "hms", # Mode Hive Metastore
        "hoodie.datasource.hive_sync.metastore.uris": "thrift://localhost:9083",
        "hoodie.datasource.hive_sync.database": "default",
        "hoodie.datasource.hive_sync.table": table_name,
        # Important car nous avons justifié au jury que nos tables ne sont pas partitionnées :
        "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.NonPartitionedExtractor"

    }
    
    # Construction du chemin complet cible (ex: s3a://silver-zone/faculty_profiles)
    target_path = f"{base_path}/{table_name}"
    
    # En Hudi, l'écriture se fait toujours en mode "append". 
    # Le moteur se charge de faire l'upsert en interne.
    df.write.format("hudi") \
        .options(**hudi_options) \
        .mode("append") \
        .save(target_path)
        
    print(f"✅ Table Hudi '{table_name}' synchronisée avec succès vers {target_path}")


def write_to_quarantine(df_rejected: DataFrame, table_name: str, quarantine_path: str):
    """
    Sauvegarde les enregistrements rejetés dans la zone de quarantaine (Dead Letter Queue).
    Format JSON simple pour faciliter l'analyse ultérieure.
    """
    if df_rejected.count() > 0:
        target_path = f"{quarantine_path}/{table_name}_rejets"
        
        df_rejected.write.format("json") \
            .mode("append") \
            .save(target_path)
            
        print(f"⚠️ {df_rejected.count()} enregistrements rejetés envoyés en quarantaine : {target_path}")
    else:
        print(f"✅ Aucun enregistrement rejeté pour {table_name}.")