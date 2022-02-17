/*
 * Copyright 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.spanner_migration;

import static org.apache.beam.sdk.io.Compression.GZIP;

import com.google.cloud.Date;
import com.google.cloud.spanner.Mutation;
import com.google.gson.Gson;
import java.io.Serializable;
import java.util.Optional;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/*
## How to run
mvn clean
mvn compile
mvn exec:java \
    -Dexec.mainClass=com.example.spanner_migration.SpannerBulkWrite \
    -Pdataflow-runner \
    -Dexec.args="--project=my-project-id \
                 --instanceId=my-instance-id \
                 --databaseId=my-database-id \
                 --table=my-table \
                 --importBucket=my-import-bucket \
                 --runner=DataflowRunner \
                 --region=your-gcp-region"
*/

@SuppressWarnings("serial")
public class SpannerBulkWrite {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerBulkWrite.class);

  public interface Options extends PipelineOptions {

    @Description("Spanner instance ID to write to")
    @Validation.Required
    String getInstanceId();

    void setInstanceId(String value);

    @Description("Spanner database name to write to")
    @Validation.Required
    String getDatabaseId();

    void setDatabaseId(String value);

    @Description("Spanner table name to write to")
    @Validation.Required
    String getTable();

    void setTable(String value);


    @Description("Location of your GCS Bucket with your exported database files")
    @Validation.Required
    String getImportBucket();

    void setImportBucket(String value);

  }

  static class ParseItems extends DoFn<String, Record> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(new Gson().fromJson(c.element(), Record.class));
      //In a production system, you should use a dead letter queue
    }
  }

  static class CreateItemMutations extends DoFn<Record, Mutation> {

    String table;

    public CreateItemMutations(String table) {
      this.table = table;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Record record = c.element();

      Mutation.WriteBuilder mutation = Mutation.newInsertOrUpdateBuilder(table);

      try {
        // [START mapping]
        mutation.set("Username").to(record.Item.Username.S);

        Optional.ofNullable(record.Item.Zipcode).ifPresent(x -> {
          mutation.set("Zipcode").to(Integer.parseInt(x.N));
        });

        Optional.ofNullable(record.Item.Subscribed).ifPresent(x -> {
          mutation.set("Subscribed").to(Boolean.parseBoolean(x.BOOL));
        });

        Optional.ofNullable(record.Item.ReminderDate).ifPresent(x -> {
          mutation.set("ReminderDate").to(Date.parseDate(x.S));
        });

        Optional.ofNullable(record.Item.PointsEarned).ifPresent(x -> {
          mutation.set("PointsEarned").to(Integer.parseInt(x.N));
        });
        // [END mapping]

        c.output(mutation.build());

      } catch (Exception ex) {
        LOG.error("Unable to create mutation", ex);
      }
    }
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    Pipeline p = Pipeline.create(options);

    // file pattern to only grab data export files (which contain dashes in filename)
    // and ignore export job metadata files
    String inputFiles = "gs://" + options.getImportBucket() + "/**/*.json.gz";

    // (Source) read DynamoDB items from export files
    PCollection<String> input = p.apply("ReadItems",
        TextIO.read().from(inputFiles).withCompression(GZIP));

    // Parse the items into objects
    PCollection<Record> items = input.apply("ParseItems", ParDo.of(new ParseItems()));

    // Create Cloud Spanner mutations using parsed Item objects
    PCollection<Mutation> mutations = items.apply("CreateItemMutations",
        ParDo.of(new CreateItemMutations(options.getTable())));

    // (Sink) write the Mutations to Spanner
    mutations.apply("WriteItems", SpannerIO.write()
        .withInstanceId(options.getInstanceId())
        .withDatabaseId(options.getDatabaseId()));

    p.run().waitUntilFinish();

  }

  // JSON mapping item to object
  // [START GSON]
  public static class Record implements Serializable {

    private Item Item;

  }

  public static class Item implements Serializable {

    private Username Username;
    private PointsEarned PointsEarned;
    private Subscribed Subscribed;
    private ReminderDate ReminderDate;
    private Zipcode Zipcode;

  }

  public static class Username implements Serializable {

    private String S;

  }

  public static class PointsEarned implements Serializable {

    private String N;

  }

  public static class Subscribed implements Serializable {

    private String BOOL;

  }

  public static class ReminderDate implements Serializable {

    private String S;

  }

  public static class Zipcode implements Serializable {

    private String N;

  }
  // [END GSON]
}
