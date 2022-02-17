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

import com.google.cloud.Date;
import com.google.cloud.spanner.Mutation;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.Serializable;
import java.util.Optional;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/*
## How to run
mvn clean
mvn compile
mvn exec:java \
    -Dexec.mainClass=com.example.spanner_migration.SpannerStreamingWrite \
    -Pdataflow-runner \
    -Dexec.args="--project=$GOOGLE_CLOUD_PROJECT \
                 --instanceId=my-instance-id \
                 --databaseId=my-database-id \
                 --table=my-table \
                 --experiments=allow_non_updatable_job \
                 --subscription=my-pubsub-subscription
                 --runner=DataflowRunner \
                 --region=your-gcp-region"
*/

@SuppressWarnings("serial")
public class SpannerStreamingWrite {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerStreamingWrite.class);

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


    @Description("Pub/Sub Subscription with streaming changes (full subscription path reqd")
    @Validation.Required
    String getSubscription();

    void setSubscription(String value);

    @Description("How often to process the window (s)")
    @Default.String("60")
    String getWindow();

    void setWindow(String value);

  }

  static class UpdateItems extends DoFn<String, Item> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      JsonObject json = JsonParser.parseString(c.element()).getAsJsonObject();
      if (json.has("NewImage")) {
        LOG.info("received a create/update");
        c.output(new Gson().fromJson(json.getAsJsonObject("NewImage"), Item.class));
      }
    }
  }

  static class DeleteItems extends DoFn<String, Item> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      JsonObject json = JsonParser.parseString(c.element()).getAsJsonObject();
      if (!json.has("NewImage")) {
        LOG.info("received a delete");
        c.output(new Gson().fromJson(json.getAsJsonObject("Keys"), Item.class));
      }
    }
  }

  static class UpdateMutations extends DoFn<Item, Mutation> {

    String table;

    public UpdateMutations(String table) {
      this.table = table;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Item item = c.element();
      LOG.info("Creating new/update Mutation for " + item.Username.S);
      Mutation.WriteBuilder mutation = Mutation.newReplaceBuilder(table);

      try {
        // [START mapping]
        mutation.set("Username").to(item.Username.S);

        Optional.ofNullable(item.Zipcode).ifPresent(x -> {
          mutation.set("Zipcode").to(Integer.parseInt(x.N));
        });

        Optional.ofNullable(item.Subscribed).ifPresent(x -> {
          mutation.set("Subscribed").to(Boolean.parseBoolean(x.BOOL));
        });

        Optional.ofNullable(item.ReminderDate).ifPresent(x -> {
          mutation.set("ReminderDate").to(Date.parseDate(x.S));
        });

        Optional.ofNullable(item.PointsEarned).ifPresent(x -> {
          mutation.set("PointsEarned").to(Integer.parseInt(x.N));
        });
        // [END mapping]

        c.output(mutation.build());

      } catch (Exception ex) {
        LOG.error("Unable to create mutation", ex);
      }

    }

  }


  static class DeleteMutations extends DoFn<Item, Mutation> {

    String table;

    public DeleteMutations(String table) {
      this.table = table;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Item item = c.element();
      LOG.info("Creating delete mutation for " + item.Username.S);
      Mutation mutation = Mutation.delete(table, com.google.cloud.spanner.Key.of(item.Username.S));
      c.output(mutation);
    }

  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    Pipeline p = Pipeline.create(options);

    // Streaming pipeline, provide full subscription path
    PCollection<String> messages = p
        .apply("Reading from PubSub",
            PubsubIO.readStrings().fromSubscription(options.getSubscription()));

    // Choose Update Type
    // create mutations for creates and updates
    PCollection<Mutation> updates = messages.apply("Create-or-Update?", ParDo.of(new UpdateItems()))
        .apply("CU->Mutations", ParDo.of(new UpdateMutations(options.getTable())));
    // create mutations for deletes
    PCollection<Mutation> deletes = messages.apply("Delete?", ParDo.of(new DeleteItems()))
        .apply("D->Mutations", ParDo.of(new DeleteMutations(options.getTable())));

    // Merge both sets of mutations
    PCollectionList<Mutation> merged = PCollectionList.of(updates).and(deletes);

    // Create fixed windows on unbounded (pub/sub) source
    PCollection<Mutation> mergedWindowed = merged.apply("Merging Mutations",
            Flatten.<Mutation>pCollections())
        .apply("Creating Windows", Window.<Mutation>into(
            FixedWindows.of(Duration.standardSeconds(Long.parseLong(options.getWindow())))));

    // commit changes to Spanner
    mergedWindowed.apply("Commit->Spanner", SpannerIO.write()
        .withInstanceId(options.getInstanceId())
        .withDatabaseId(options.getDatabaseId()));

    //p.run().waitUntilFinish(); //needed when running local
    p.run(); //when using Cloud Dataflow runner

  }


  // JSON mapping item to object
  // [START GSON]
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
