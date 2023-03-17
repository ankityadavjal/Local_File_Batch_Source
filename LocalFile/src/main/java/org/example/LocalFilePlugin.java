package org.example;/*
 * Copyright © 2023 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.SourceInputFormatProvider;
import io.cdap.plugin.common.batch.JobUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import java.net.URISyntaxException;

@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(LocalFilePlugin.NAME)
@Description("Reads data from Local File, generates schemas for csv and tsv only.")
public class LocalFilePlugin extends BatchSource<LongWritable, Text, StructuredRecord> {
    public static final String NAME = "LocalFilePlugin";

    private LocalFileConfig config;
    public static final Schema DEFAULT_SCHEMA=Schema.recordOf("event", Schema.Field.of("offset", Schema.of(Schema.Type.LONG)), Schema.Field.of("body", Schema.of(Schema.Type.STRING)));


    public LocalFilePlugin(LocalFileConfig config) {
        this.config = config;
    }

    @Override
    public void prepareRun(BatchSourceContext batchSourceContext) throws Exception {
        FailureCollector failureCollector = batchSourceContext.getFailureCollector();
        failureCollector.getOrThrowException();
        setJob(batchSourceContext);
    }
    private void setJob(BatchSourceContext batchSourceContext) throws IOException {
        Gson gson = new GsonBuilder().create();
        Job job = JobUtils.createInstance();
        Configuration jobConfig = job.getConfiguration();
        jobConfig.set(config.getReferenceName(), gson.toJson(config));
        TextInputFormat.addInputPath(job, new Path (config.getFilePath()));

        batchSourceContext.setInput(Input.of(config.getReferenceName(), new SourceInputFormatProvider(TextInputFormat.class, jobConfig)));
    }

    private Schema getOutputSchema() throws IOException {
        if(config.getNameFormat().equals("csv"))
        {
            FileReader fr=new FileReader(config.getFilePath());
            try(BufferedReader br=new BufferedReader(fr))
            {
                   return generateSchema(br.readLine(),config.getDelimiter());
            }
        }
        else if(config.getNameFormat().equals("tsv"))
        {
            FileReader fr=new FileReader(config.getFilePath());
            try(BufferedReader br=new BufferedReader(fr))
            {
                return generateSchema(br.readLine(),config.getDelimiter());
            }
        }

        return DEFAULT_SCHEMA;
    }
    public Schema generateSchema(String csvFileString, String delimiter) {
        String FirstLine = csvFileString.split("\n")[0]; //if csvString has multiple lines consider first
        String[] headerValues = FirstLine.split(delimiter);

        List<Schema.Field> schemaFields = new ArrayList<>();

        for (String headerName : headerValues) {
            schemaFields.add(Schema.Field.of(headerName, Schema.of(Schema.Type.STRING)));
        }
        return Schema.recordOf("event", schemaFields);
    }


    public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
        FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
        config.validate(failureCollector);
        try {
            pipelineConfigurer.getStageConfigurer().setOutputSchema(getOutputSchema());
        } catch (IOException e) {
            failureCollector.addFailure(e.getMessage(), null);
            failureCollector.getOrThrowException();
        }

    }

    public void transform(KeyValue<LongWritable, Text> input, Emitter<StructuredRecord> emitter) throws Exception {
        StructuredRecord.Builder builder = StructuredRecord.builder(getOutputSchema());
        if (!config.getNameIncludeHeaders() && input.getKey().get() == 0)
            return;
        if(config.getNameFormat().equals("csv"))
        {
               csvTransformation(input,builder);
        }
        else if (config.getNameFormat().equals("tsv"))
        {
               tsvTransformation(input,builder);
        }
        else
        fileTransformation(input, builder);

        emitter.emit(builder.build());
    }
    private void fileTransformation(KeyValue<LongWritable, Text> input, StructuredRecord.Builder builder) throws IOException {
        builder.set(getOutputSchema().getFields().get(0).getName(), input.getKey().get());
        builder.set(getOutputSchema().getFields().get(1).getName(), input.getValue().toString());
    }
    private void csvTransformation(KeyValue<LongWritable, Text> input, StructuredRecord.Builder builder) throws IOException {
           FileTransform(input,builder);
        }


    private void tsvTransformation(KeyValue<LongWritable, Text> input, StructuredRecord.Builder builder) throws IOException {
        FileTransform(input,builder);
        }

        private void FileTransform(KeyValue<LongWritable, Text> input, StructuredRecord.Builder builder) throws IOException {
            int i=0;
            String[] result=input.getValue().toString().split(config.getDelimiter());
            List<Schema.Field> fields = getOutputSchema().getFields();
            for (Schema.Field field : fields) {
                String name=field.getName();
                builder.set(name, result[i]);
                i++;
            }
    }



}
