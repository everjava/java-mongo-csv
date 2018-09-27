package br.com.javamongocsv.tests;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;

/**
 * Unit test for simple App.
 */
public class AppTest {

	MongoDatabase mongoDatabase;
	MongoClient mongoClient;

	@Before
	public void setUp() {
		mongoClient = new MongoClient();
		mongoDatabase = mongoClient.getDatabase("everson");
	}

	@Test
	public void main() {
		try {
			// File input = new
			// File("/private/var/lib/cobdigital/temp/data/CUSTOMERS_CAMP_EMAIL_PROGRAMADO_20180718.CSV");
			//File input = new File("/private/var/lib/cobdigital/temp/data-json/CUSTOMERS_CAMP_EMAIL.CSV");
			File input = new File("/private/var/lib/cobdigital/temp/data/LOAD_CUSTOMER_23.CSV");
			

			List<Map<?, ?>> data = readObjectsFromCsv(input);
			// writeAsJson(data);
			bulk2(data);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public List<Map<?, ?>> readObjectsFromCsv(File file) throws IOException {
		CsvSchema bootstrap = CsvSchema.emptySchema().withHeader();
		CsvMapper csvMapper = new CsvMapper();
		MappingIterator<Map<?, ?>> mappingIterator = csvMapper.reader(Map.class).with(bootstrap).readValues(file);

		return mappingIterator.readAll();
	}

	@SuppressWarnings("unused")
	public void bulk2(List<Map<?, ?>> data) throws Exception {
		MongoCollection<Document> bulkCollection = mongoDatabase.getCollection("bulkCollection");
		// Bulk operations
		List<WriteModel<Document>> writes = new ArrayList<WriteModel<Document>>();
		
		for (Map<?, ?> map : data) {
			ObjectMapper mapper = new ObjectMapper();
			mapper.writeValueAsString(map);
			System.out.println(mapper.writeValueAsString(map));
			
			//mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
			//mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
			//mapper.setConfig(new DeserializationConfig().with(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT)  );
			 //mapper.setSerializationInclusion(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY);
			 mapper = mapper.setSerializationInclusion(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL);
			 
			 
			
			String jsonString = mapper.writeValueAsString(map).replaceAll("[\\[-\\]]", "");
			System.out.println(jsonString);
			Document document = Document.parse(jsonString);
			writes.add(new InsertOneModel<Document>(document));
		}
		bulkCollection.drop();
		bulkCollection.bulkWrite(writes);
	}

	public void bulk(List<Map<?, ?>> data) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		mapper.writeValueAsString(data);
		System.out.println(mapper.writeValueAsString(data));
		String jsonString = mapper.writeValueAsString(data).replaceAll("[\\[-\\]]", "");
		System.out.println(jsonString);
		Document document = Document.parse(jsonString);

		MongoCollection<Document> bulkCollection = mongoDatabase.getCollection("bulkCollection");
		// Bulk operations
		List<WriteModel<Document>> writes = new ArrayList<WriteModel<Document>>();
		// writes.add(new InsertOneModel<Document>(new Document("_id", 4)));
		// writes.add(new InsertOneModel<Document>(new Document("_id", 5)));
		// writes.add(new InsertOneModel<Document>(new Document("_id", 6)));
		// writes.add(new UpdateOneModel<Document>(new Document("_id", 1), new
		// Document("$set", new Document("x", 2))));
		// writes.add(new DeleteOneModel<Document>(new Document("_id", 2)));
		// writes.add(new ReplaceOneModel<Document>(new Document("_id", 3), new
		// Document("_id", 3).append("x", 4)));
		writes.add(new InsertOneModel<Document>(document));
		// 1. Ordered bulk operation - order is guarenteed
		bulkCollection.drop();
		bulkCollection.bulkWrite(writes);
	}

	public void writeAsJson(List<Map<?, ?>> data) throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		mapper.writeValueAsString(data);
		System.out.println(mapper.writeValueAsString(data));
		String jsonString = mapper.writeValueAsString(data);
		Document document = Document.parse(jsonString);
		//
		// String[] slitter = jsonString.split(",");
		// for (int i = 0; i < slitter.length; i++) {
		// String string = slitter[i];
		// Bs
		//
		// }
		// mongoDatabase.getCollection("camel_test").insertOne(document);
		// new Document().

		//
		// DBCollection countersCollection = mongoDatabase.getCollection("counters");
		//
		// List<WriteModel<Document>> writes = new ArrayList<WriteModel<Document>>();

	}

}
