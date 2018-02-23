package com.workday.kafka.server;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by drewwilson on 23/02/2018.
 */
public class SimpleTest {
    @Test
    public void testSimple() throws IOException {
        InputStream resourceAsStream = this.getClass().getResourceAsStream("/Employee.avsc");
        final Schema schema = new Schema.Parser().parse(resourceAsStream);
        GenericRecord x = new GenericData.Record(schema);
        x.put("id",123);
        GenericDatumWriter<Object> writer = new GenericDatumWriter<>(schema);
        DataFileWriter<Object> fileWriter = new DataFileWriter<>(writer);
        fileWriter.create(schema, new File("Andrew.avro"));
        fileWriter.append(x);
        fileWriter.close();

        File file = new File("Andrew.avro");
        GenericDatumReader<Object> reader = new GenericDatumReader<>();
        DataFileReader<Object> objects = new DataFileReader<>(file, reader);
        while(objects.hasNext()) {
            Object next = objects.next();
            System.out.println("***" + next);
        }

    }

}
