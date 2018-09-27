package br.com.javamongocsv;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;


/**
 * Hello world!
 *
 */
public class App 
{
	public static void main(String[] args) throws Exception {
        File input = new File("/private/var/lib/cobdigital/temp/data/CUSTOMERS_CAMP_EMAIL_PROGRAMADO_20180718.CSV");
        //File output = new File("/x/data.json");

        List<Map<?, ?>> data = readObjectsFromCsv(input);
        writeAsJson(data );
    }

    public static List<Map<?, ?>> readObjectsFromCsv(File file) throws IOException {
        CsvSchema  bootstrap = CsvSchema.emptySchema().withHeader();
        CsvMapper csvMapper = new CsvMapper();
        MappingIterator<Map<?, ?>> mappingIterator = csvMapper.reader(Map.class).with(bootstrap).readValues(file);

        return mappingIterator.readAll();
    }

    public static void writeAsJson(List<Map<?, ?>> data ) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.writeValueAsString(data );
        System.out.println(mapper.writeValueAsString(data ));
    }
}
