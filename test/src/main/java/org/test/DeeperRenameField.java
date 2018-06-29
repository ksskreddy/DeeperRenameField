package org.test;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Field; 
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;


import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;


import java.util.Map;

public abstract  class DeeperRenameField<R extends ConnectRecord<R>> implements Transformation<R>{
	
	public static final String OVERVIEW_DOC = "Rename fields which may not be only on TopLevel"
            + "<p/>Use the concrete transformation type designed for the record key (<code>" + Key.class.getName() + "</code>) "
            + "or value (<code>" + Value.class.getName() + "</code>).";

	interface ConfigName {
        String CURRENTNAME = "rename.fieldname";
        String NEWNAME = "rename.newname";
    }
	
	
	private static final String CURRENTNAME_DOC="dot seperated fields representing the full path to field whose name is required to change";
	private static final String CURRENTNAME_DEFAULT="";
	private static final String NEWNAME_DOC="New Name for the above field";
	private static final String NEWNAME_DEFAULT="";
	
	
	private String currentname;
	private String newname;
	private String reversename;
	private String reversenewname;
	
	
	public static final ConfigDef CONFIG_DEF = new ConfigDef()
			.define(ConfigName.CURRENTNAME,ConfigDef.Type.STRING, CURRENTNAME_DEFAULT, ConfigDef.Importance.HIGH, CURRENTNAME_DOC)
			.define(ConfigName.NEWNAME,ConfigDef.Type.STRING, NEWNAME_DEFAULT, ConfigDef.Importance.HIGH, NEWNAME_DOC);
	
	
	 private static final String PURPOSE = "Rename deeper fields";
	 private Cache<Schema, Schema> schemaUpdateCache;
	 
	public void configure(Map<String, ?> configs) {
		 final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
		 currentname=config.getString(ConfigName.CURRENTNAME);
		 newname=config.getString(ConfigName.NEWNAME);
		 int lastdot=currentname.lastIndexOf('.');
		 if(lastdot==-1)  
		 {
			 reversename=newname;
			 reversenewname=currentname;
		 }
		 else {
			 reversename=currentname.substring(0,lastdot+1)+newname;
			 reversenewname=currentname.substring(lastdot+1);
		 }
		 schemaUpdateCache = new SynchronizedCache(new LRUCache<Schema, Schema>(16));
		
	}

	public R apply(R record) {
		if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
	}

	private R applyWithSchema(R record) {
		
		final Struct value = requireStruct(operatingValue(record), PURPOSE);

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }
        
        final Struct updatedValue = new Struct(updatedSchema);
        for (Field field : updatedSchema.fields()) {
        		if(!reversename.startsWith(field.name()))
        		{
        			final Object fieldValue = value.get(field.name());
                 updatedValue.put(field.name(), fieldValue);
        		}
        		else
        		{
        			final Object fieldValue = value.get(getOldName(field.name(),field.name()));
        			Object newstruct = RecursiveBuildValue(field,field.name(),fieldValue);
        			updatedValue.put(field.name(), newstruct);
        		}
        				
        }
        return newRecord(record, updatedSchema, updatedValue);
	}
	
	
	

	private Object RecursiveBuildValue(Field field, String name,Object fieldValue) {
		
		if(name.equals(reversename)) return fieldValue; 
		if(!(fieldValue instanceof Struct)) return fieldValue;
		Schema schema=field.schema();
		
		final Struct newstruct = new Struct(schema);
		
		for(Field field1 : schema.fields())
		{
			String temp=name+"."+ field1.name();
			if (!reversename.startsWith((temp))) {
				final Object newfieldValue = ((Struct)fieldValue).get(field1.name());
				newstruct.put(field1.name(),newfieldValue);
            }
            else {
            		Object  newstruct1 = RecursiveBuildValue(field1,temp,((Struct)fieldValue).get(getOldName(temp,field1.name())));
            		newstruct.put(field1.name(), newstruct1);
            }
		}
		
		return newstruct;
			
	}

	private R applySchemaless(R record) {
		//if no schema no concept of hierarchy
		return record;
	}

	public ConfigDef config() {
		return CONFIG_DEF;
	}

	
	
	private String getNewName(String fullpath,String fieldname)
	{
		if(fullpath.equals(currentname)) return newname;
		else return fieldname;
	}
	
	
	private String getOldName(String fullpath,String fieldname)
	{
		if(fullpath.equals(reversename)) return reversenewname;
		else return fieldname;
	}
	
	private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        for (Field field : schema.fields()) {
            if (!currentname.startsWith((field.name()))) {
                builder.field((field.name()), field.schema());
            }
            else {
            		Schema newschema = RecursiveUpdateSchema(field,field.name());
            		builder.field(getNewName(field.name(),field.name()),newschema);
            }
        }
        return builder.build();
    }
	
	private Schema RecursiveUpdateSchema(Field field, String name) {
		if(name.equals(currentname)) return field.schema();
		final SchemaBuilder builder = SchemaUtil.copySchemaBasics(field.schema(), SchemaBuilder.struct());
		Schema schema=field.schema();
		
		for(Field field1 : schema.fields())
		{
			String temp=name+"."+ field1.name();
			if (!currentname.startsWith((temp))) {
                builder.field((field1.name()), field1.schema());
            }
            else {
            		Schema newschema = RecursiveUpdateSchema(field1,temp);
            		builder.field(getNewName(temp,field1.name()),newschema);
            }
		}
			
		return builder.build();
	}

	public void close() {	
		
	}
	
	protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);	
	
    public static class Key<R extends ConnectRecord<R>> extends DeeperRenameField<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }

    }

    public static class Value<R extends ConnectRecord<R>> extends DeeperRenameField<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }

    }
	
	
    
}
