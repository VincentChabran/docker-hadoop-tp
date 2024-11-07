ADD JAR /opt/hive/lib/mongo-java-driver-3.12.10.jar;
ADD JAR /opt/hive/lib/mongo-hadoop-core-2.0.2.jar;
ADD JAR /opt/hive/lib/mongo-hadoop-hive-2.0.2.jar;


CREATE EXTERNAL TABLE IF NOT EXISTS marketing (
   Age INT,
   Sexe STRING,
   Taux DOUBLE,
   SituationFamiliale STRING,
   NbEnfantsAcharge INT,
   `2eme_voiture` BOOLEAN
)
STORED BY 'com.mongodb.hadoop.hive.MongoStorageHandler'
WITH SERDEPROPERTIES (
   "mongo.columns.mapping" = "{\"Age\":\"Age\",\"Sexe\":\"Sexe\",\"Taux\":\"Taux\",\"SituationFamiliale\":\"SituationFamiliale\",\"NbEnfantsAcharge\":\"NbEnfantsAcharge\",\"2eme_voiture\":\"2eme_voiture\"}"
)
TBLPROPERTIES (
   "mongo.uri" = "mongodb://mongodb:27017/votre_base_de_donnees.votre_collection"
);
