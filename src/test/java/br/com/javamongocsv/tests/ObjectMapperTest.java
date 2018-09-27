package br.com.javamongocsv.tests;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.DefaultSerializerProvider;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;

public class ObjectMapperTest {
	
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
			File input = new File("/private/var/lib/cobdigital/temp/data/LOAD_CUSTOMER_23.CSV");
			

			List<Map<?, ?>> data = readObjectsFromCsv(input);
			// writeAsJson(data);
			bulk2(data);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("unused")
	public void bulk2(List<Map<?, ?>> data) throws Exception {
		MongoCollection<Document> bulkCollection = mongoDatabase.getCollection("bulkCollection");
		// Bulk operations
		List<WriteModel<Document>> writes = new ArrayList<WriteModel<Document>>();
		
		for (Map<?, ?> map : data) {
			
			ObjectMapper mapper = new ObjectMapper();
			mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
			 
			DefaultSerializerProvider sp = new DefaultSerializerProvider.Impl();
			sp.setNullValueSerializer(new NullSerializer());//qdo o valor for nullo setNullValueSerializer
			mapper.setSerializerProvider(sp);
			TypeReference<HashMap<String, String>> typeRef = new TypeReference<HashMap<String, String>>() {};
			
			//HashMap<String, String> outputMap = mapper.readValue(mapper.writeValueAsString(map).replaceAll("[\\[-\\]]", ""), typeRef);
			
			//mapper.writeValueAsString(map);
			System.out.println(mapper.writeValueAsString(map));
			 
			String jsonString = mapper.writeValueAsString(map).replaceAll("[\\[-\\]]", "");
	 
			Document document = Document.parse(jsonString);
			writes.add(new InsertOneModel<Document>(document));
		}
		bulkCollection.drop();
		bulkCollection.bulkWrite(writes);
	}

	public List<Map<?, ?>> readObjectsFromCsv(File file) throws IOException {
		CsvSchema bootstrap = CsvSchema.emptySchema().withHeader();
		CsvMapper csvMapper = new CsvMapper();
		MappingIterator<Map<?, ?>> mappingIterator = csvMapper.reader(Map.class).with(bootstrap).readValues(file);

		return mappingIterator.readAll();
	}
	
	
	

	//@Test
	public void test_Map의_null_EmptyString_변환() throws IOException {
		try {
			HashMap<String, String> testMap = new HashMap<>();
			testMap.put("test1", "");
			//testMap.put("test2", null);
			//testMap.put("test3", "33");

			ObjectMapper mapper = new ObjectMapper();
			mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
			 
			DefaultSerializerProvider sp = new DefaultSerializerProvider.Impl();
			sp.setNullValueSerializer(new NullSerializer());//qdo o valor for nullo setNullValueSerializer
			mapper.setSerializerProvider(sp);
			
			
			
			TypeReference<HashMap<String, String>> typeRef = new TypeReference<HashMap<String, String>>() {};
			HashMap<String, String> outputMap = mapper.readValue(mapper.writeValueAsString(testMap), typeRef);

			System.out.println("teste1= " + outputMap.get("test1"));// 4
			System.out.println("teste2= " + outputMap.get("test2"));// null
			System.out.println("teste3= " + outputMap.get("test3"));// 33
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public class NullSerializer extends JsonSerializer<Object> {
		public void serialize(Object value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
			// jgen.writeString("");
			System.err.println("valor = " + value);
			if(value == null || value == "") {
				jgen.writeNull();
			}else {
			jgen.writeString(value.toString());
			}
		}
	}
}
