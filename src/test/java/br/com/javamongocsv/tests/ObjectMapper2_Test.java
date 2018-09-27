package br.com.javamongocsv.tests;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
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
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;

public class ObjectMapper2_Test {

	MongoDatabase mongoDatabase;
	MongoClient mongoClient;

	@Before
	public void setUp() {
		mongoClient = new MongoClient();
		mongoDatabase = mongoClient.getDatabase("everson");
	}

	public List<Map<?, ?>> readObjectsFromCsv(File file) throws IOException {
		CsvSchema bootstrap = CsvSchema.emptySchema().withHeader();
		CsvMapper csvMapper = new CsvMapper();
		MappingIterator<Map<?, ?>> mappingIterator = csvMapper.reader(Map.class).with(bootstrap).readValues(file);

		return mappingIterator.readAll();
	}

	public class NullSerializer extends JsonSerializer<Object> {
		public void serialize(Object value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
			// jgen.writeString("");
			System.err.println("valor = " + value);
			if (value == null || value == "") {
				jgen.writeNull();
			} else {
				jgen.writeString(value.toString());
			}
		}
	}

	public List<Map<?, ?>> readObjectsFromLine(String line) throws IOException {
		StringBuffer header = new StringBuffer();
		header.append(
				"NOME,SOBRENOME,DOCUMENTO,CONTRATO,NO_FATURA,DESCRICAO_FATURA,DT_VENCIMENTO,VALOR_FATURA,ENDERECO,COMPLEMENTO,BAIRRO,CEP,CIDADE,UF,CEL_1,CEL_2,CEL_3,CEL_4,FIXO_1,FIXO_2,FIXO_3,FIXO_4,EMAIL_1,EMAIL_2,EMAIL_3,EMAIL_4");
		header.append("\n");
		header.append(line);

		CsvSchema bootstrap = CsvSchema.emptySchema().withHeader();
		CsvMapper csvMapper = new CsvMapper();
		MappingIterator<Map<?, ?>> mappingIterator = csvMapper.reader(Map.class).with(bootstrap)
				.readValues(header.toString());

		return mappingIterator.readAll();
	}

	@Test
	public void arquvox() throws Exception {
		BufferedReader reader = null;
		try {
			File file = new File("/private/var/lib/cobdigital/temp/data/LOAD_CUSTOMER_45.CSV");
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "ISO-8859-1"));

			MongoCollection<Document> bulkCollection = mongoDatabase.getCollection("bulkCollection");
			bulkCollection.drop();

			// Bulk operations
			List<WriteModel<Document>> writes = new ArrayList<WriteModel<Document>>();

			String line = "";
			int cont = 0;
			while ((line = reader.readLine()) != null) {

				if (cont == 0) {
					cont++;
					continue;
				}

				List<Map<?, ?>> data = readObjectsFromLine(line);

				ObjectMapper mapper = new ObjectMapper();
				mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

				DefaultSerializerProvider sp = new DefaultSerializerProvider.Impl();
				sp.setNullValueSerializer(new NullSerializer());
				mapper.setSerializerProvider(sp);
				TypeReference<HashMap<String, String>> typeRef = new TypeReference<HashMap<String, String>>() {
				};

				System.out.println(mapper.writeValueAsString(data));
				String jsonString = mapper.writeValueAsString(data).replaceAll("[\\[-\\]]", "");
				Document document = Document.parse(jsonString);
				writes.add(new InsertOneModel<Document>(document));
				cont++;
				if (cont == 3000) {
					bulkCollection.bulkWrite(writes, new BulkWriteOptions().ordered(false));
					cont = 0;
					writes = new ArrayList<WriteModel<Document>>();
				}

			}

			if (cont > 0) {
				bulkCollection.bulkWrite(writes, new BulkWriteOptions().ordered(false));
				cont = 0;
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			reader.close();
		}

	}

}
