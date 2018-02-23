package com.workday.kafka.server;


import okhttp3.*;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

@Ignore(value="This is an integration test, but I'd like to get it fixed up, with an embedded SchemaServer.")
public class SchemaTest {
    public static final String LOCAL_SERVER = "http://localhost:8081/";

    private final static MediaType SCHEMA_CONTENT = MediaType.parse("application/vnd.schemaregistry.v1+json");

    // @TODO note this is a bit ugly, all the schema stuff needs to be quoted with \
    private final static String EMPLOYEE_SCHEMA = "{\n" +
            "  \"schema\": \"" +
            "  {" +
            "    \\\"namespace\\\": \\\"com.cloudurable.phonebook\\\"," +
            "    \\\"type\\\": \\\"record\\\"," +
            "    \\\"name\\\": \\\"Employee\\\"," +
            "    \\\"fields\\\": [" +
            "        {\\\"name\\\": \\\"fName\\\", \\\"type\\\": \\\"string\\\"}," +
            "        {\\\"name\\\": \\\"lName\\\", \\\"type\\\": \\\"string\\\"}," +
            "        {\\\"name\\\": \\\"age\\\",  \\\"type\\\": \\\"int\\\"}," +
            "        {\\\"name\\\": \\\"phoneNumber\\\",  \\\"type\\\": \\\"string\\\"}" +
            "    ]" +
            "  }\"" +
            "}";

    // @todo we need to sort out all the asserts here.
    @Test public void testSchema() throws IOException {

        final OkHttpClient client = new OkHttpClient();

        //POST A NEW SCHEMA
        Request request = new Request.Builder()
                .post(RequestBody.create(SCHEMA_CONTENT, EMPLOYEE_SCHEMA))
                .url("http://localhost:8081/subjects/Employee/versions")
                .build();

        String output = client.newCall(request).execute().body().string();
        System.out.println(output);

        //LIST ALL SCHEMAS
        request = new Request.Builder()
                .url(LOCAL_SERVER +"subjects")
                .build();

        output = client.newCall(request).execute().body().string();
        System.out.println(output);


        //SHOW ALL VERSIONS OF EMPLOYEE
        request = new Request.Builder()
                .url(LOCAL_SERVER +"subjects/Employee/versions/")
                .build();

        output = client.newCall(request).execute().body().string();
        System.out.println(output);

        //SHOW VERSION 2 OF EMPLOYEE
        request = new Request.Builder()
                .url(LOCAL_SERVER +"subjects/Employee/versions/2")
                .build();

        output = client.newCall(request).execute().body().string();
        System.out.println(output);

        //SHOW THE SCHEMA WITH ID 3
        request = new Request.Builder()
                .url(LOCAL_SERVER +"schemas/ids/3")
                .build();

        output = client.newCall(request).execute().body().string();
        System.out.println(output);


        //SHOW THE LATEST VERSION OF EMPLOYEE 2
        request = new Request.Builder()
                .url(LOCAL_SERVER +"subjects/Employee/versions/latest")
                .build();

        output = client.newCall(request).execute().body().string();
        System.out.println(output);


        //CHECK IF SCHEMA IS REGISTERED
        request = new Request.Builder()
                .post(RequestBody.create(SCHEMA_CONTENT, EMPLOYEE_SCHEMA))
                .url(LOCAL_SERVER +"subjects/Employee")
                .build();

        output = client.newCall(request).execute().body().string();
        System.out.println(output);


        //TEST COMPATIBILITY
        request = new Request.Builder()
                .post(RequestBody.create(SCHEMA_CONTENT, EMPLOYEE_SCHEMA))
                .url(LOCAL_SERVER +"compatibility/subjects/Employee/versions/latest")
                .build();

        output = client.newCall(request).execute().body().string();
        System.out.println(output);

        // TOP LEVEL CONFIG
        request = new Request.Builder()
                .url(LOCAL_SERVER +"config")
                .build();

        output = client.newCall(request).execute().body().string();
        System.out.println(output);


        // SET TOP LEVEL CONFIG
        // VALUES are none, backward, forward and full
        request = new Request.Builder()
                .put(RequestBody.create(SCHEMA_CONTENT, "{\"compatibility\": \"none\"}"))
                .url(LOCAL_SERVER +"config")
                .build();

        output = client.newCall(request).execute().body().string();
        System.out.println(output);

        // SET CONFIG FOR EMPLOYEE
        // VALUES are none, backward, forward and full
        request = new Request.Builder()
                .put(RequestBody.create(SCHEMA_CONTENT, "{\"compatibility\": \"backward\"}"))
                .url(LOCAL_SERVER +"config/Employee")
                .build();

        output = client.newCall(request).execute().body().string();
        System.out.println(output);

        request = new Request.Builder()
                .delete()
                .url(LOCAL_SERVER +"subjects/Employee")
                .build();

        output = client.newCall(request).execute().body().string();
        // The return is an array of the versions deleted.
        System.out.println(output);



    }
}

