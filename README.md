# DeeperRenameField
Kafka connect Transform which can rename deep level fields in the schema

# Details
   It has two properties to be configured
        
         1. rename.fieldname -> A dotted String representing full path to field whose name to be changed
         2. rename.newname  -> New Name for the field.

## How to Compile 
   - It is a maven project.Compile using
   - `mvn clean package`
   - A jar named <b>test-0.0.1-SNAPSHOT.jar</b> will be generated in the target folder.Put this jar in the kafka connect plugin path directory.

## How to Use
   Lets us consider the following scenario
      Our schema contains a field named **source** and it contains a field named **version**.We want to change the **version** to **VersionNumber**.
      Include the following properties while defining the connector to get the above effect.
      
         "transforms": "route"
         "transforms.route.type":"org.test.DeeperRenameField$Value"
         "transforms.route.rename.fieldname":"source.version"
         "transforms.route.rename.newname":"VersionNumber"
