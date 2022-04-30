/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.examples;

// beam-playground:
//   name: MinimalWordCount
//   description: An example that counts words in Shakespeare's works.
//   multifile: false
//   pipeline_options:
//   categories:
//     - Combiners
//     - Filtering
//     - IO
//     - Core Transforms

import java.util.ArrayList;

import java.util.Arrays;
import java.util.Collection;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * An example that counts words in Shakespeare.
 *
 * <p>This class, {@link MinimalWordCount}, is the first in a series of four successively more
 * detailed 'word count' examples. Here, for simplicity, we don't show any error-checking or
 * argument processing, and focus on construction of the pipeline, which chains together the
 * application of core transforms.
 *
 * <p>Next, see the {@link WordCount} pipeline, then the {@link DebuggingWordCount}, and finally the
 * {@link WindowedWordCount} pipeline, for more detailed examples that introduce additional
 * concepts.
 *
 * <p>Concepts:
 *
 * <pre>
 *   1. Reading data from text files
 *   2. Specifying 'inline' transforms
 *   3. Counting items in a PCollection
 *   4. Writing data to text files
 * </pre>
 *
 * <p>No arguments are required to run this pipeline. It will be executed with the DirectRunner. You
 * can see the results in the output files in your current working directory, with names like
 * "wordcounts-00001-of-00005. When running on a distributed service, you would use an appropriate
 * file service.
 */
public class MinimalPageRankPriyanka {
   static class Job1Finalizer extends DoFn<KV<String, Iterable<String>>, KV<String, RankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<String>> element,
        OutputReceiver<KV<String, RankedPage>> receiver) {
      Integer contributorVotes = 0;
      if (element.getValue() instanceof Collection) {
        contributorVotes = ((Collection<String>) element.getValue()).size();
      }
      ArrayList<VotingPage> voters = new ArrayList<VotingPage>();
      for (String voterName : element.getValue()) {
        if (!voterName.isEmpty()) {
          voters.add(new VotingPage(voterName, contributorVotes));
        }
      }
      receiver.output(KV.of(element.getKey(), new RankedPage(element.getKey(), voters)));
    }
  }
    static class Job2Mapper extends DoFn<KV<String, RankedPage>, KV<String, RankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, RankedPage> element,
        OutputReceiver<KV<String, RankedPage>> receiver) {
      Integer votes = 0;

      ArrayList<VotingPage> voters = element.getValue().getVoters();
      if(voters instanceof Collection){
         votes = ((Collection<VotingPage>)voters).size();
      }

      for(VotingPage v: voters){
        String pageName = v.getName();
        Double pageRank = v.getRank();
        String contributingPageName = element.getKey();
        Double contributingPageRank = element.getValue().getRank();
        VotingPage contributor = new VotingPage(contributingPageName, contributingPageRank, votes);
        ArrayList<VotingPage> arr = new ArrayList<VotingPage>();
        arr.add(contributor);
        receiver.output(KV.of(v.getName(), new RankedPage(pageName,pageRank,arr)));
      }
  }
}

  static class Job2Updater extends DoFn<KV<String, Iterable<RankedPage>>, KV<String, RankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<RankedPage>> element,
        OutputReceiver<KV<String, RankedPage>> receiver) {
          String page = element.getKey();
          Iterable<RankedPage> rankedPages = element.getValue();
          Double dampingFactor = 0.85;
          Double updatedRank = (1-dampingFactor);
          ArrayList<VotingPage> newVoters = new ArrayList<VotingPage>();
          for(RankedPage pg : rankedPages){
            if(pg != null){
              for(VotingPage vPage : pg.getVoters()){
                newVoters.add(vPage);
                updatedRank += (dampingFactor) * vPage.getRank() / (double)vPage.getVotes();
              }
            }
          }
          receiver.output(KV.of(page, new RankedPage(page, updatedRank, newVoters)));
    }
  }


  public static void main(String[] args) {

    // Create a PipelineOptions object. This object lets us set various execution
    // options for our pipeline, such as the runner you wish to use. This example
    // will run with the DirectRunner by default, based on the class path configured
    // in its dependencies.
    PipelineOptions options = PipelineOptionsFactory.create();

    // In order to run your pipeline, you need to make following runner specific changes:
    //
    // CHANGE 1/3: Select a Beam runner, such as BlockingDataflowRunner
    // or FlinkRunner.
    // CHANGE 2/3: Specify runner-required options.
    // For BlockingDataflowRunner, set project and temp location as follows:
    //   DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
    //   dataflowOptions.setRunner(BlockingDataflowRunner.class);
    //   dataflowOptions.setProject("SET_YOUR_PROJECT_ID_HERE");
    //   dataflowOptions.setTempLocation("gs://SET_YOUR_BUCKET_NAME_HERE/AND_TEMP_DIRECTORY");
    // For FlinkRunner, set the runner as follows. See {@code FlinkPipelineOptions}
    // for more details.
    //   options.as(FlinkPipelineOptions.class)
    //      .setRunner(FlinkRunner.class);

    // Create the Pipeline object with the options we defined above
    Pipeline p = Pipeline.create(options);

    // Concept #1: Apply a root transform to the pipeline; in this case, TextIO.Read to read a set
    // of input text files. TextIO.Read returns a PCollection where each element is one line from
    // the input text (a set of Shakespeare's texts).

    // This example reads from a public dataset containing the text of King Lear.
    //
    // DC: We don't need king lear....
    // We want to read from a folder - assign to a variable since it may change.
    // We want to read from a file - just one - we need the file name - assign to a variable. 

    String dataFolder = "web04";
   //  String dataFile = "go.md";
    // String dataPath = dataFolder + "/" + dataFile;
    //p.apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/kinglear.txt"))

    
    PCollection<KV<String, String>> pcollectionKV1 = Priyankap1(p,"go.md",dataFolder);
    PCollection<KV<String, String>> pcollectionKV2 = Priyankap1(p,"java.md",dataFolder);
    PCollection<KV<String, String>> pcollectionKV3 = Priyankap1(p,"python.md",dataFolder);
    PCollection<KV<String, String>> pcollectionKV4 = Priyankap1(p,"README.md",dataFolder);

    
    PCollectionList<KV<String, String>> pCollectionKVlist = PCollectionList.of(pcollectionKV1).and(pcollectionKV2).and(pcollectionKV3).and(pcollectionKV4);
    PCollection<KV<String, String>> mergedList = pCollectionKVlist.apply(Flatten.<KV<String, String>>pCollections());
      PCollection<KV<String, RankedPage>> job2in = PCGrpList.apply(ParDo.of(new Job1Finalizer()));


   PCollection<KV<String, RankedPage>> newUpdatedOutput = null;
   PCollection<KV<String, RankedPage>> mappedKVPairs = null;
     
     int iterations = 40;
   for(int i=0; i<iterations; i++){
     if(i==0){
       mappedKVPairs = job2in.apply(ParDo.of(new Job2Mapper()));
     }else{
       mappedKVPairs = newUpdatedOutput.apply(ParDo.of(new Job2Mapper()));
     }
     PCollection<KV<String, Iterable<RankedPage>>> reducedKVPairs = mappedKVPairs.apply(GroupByKey.<String, RankedPage>create());
     newUpdatedOutput = reducedKVPairs.apply(ParDo.of(new Job2Updater()));
    }
     
    PCollection<String> pCollectionLink = mergedList.apply(
        MapElements.into(TypeDescriptors.strings()).via((myMergeLstout) -> myMergeLstout.toString()));
       // By default, it will write to a set of files with names like wordcounts-00001-of-00005
        //longLinkLines.apply(TextIO.write().to("pageRankPriyanka"));
        pCollectionLink.apply(TextIO.write().to("PriyankaOutput"));
      
    p.run().waitUntilFinish();
  }

  private static PCollection<KV<String, String>> Priyankap1(Pipeline p, String dataFile, String dataFolder) {
    String dataPath = dataFolder + "/" + dataFile;

    PCollection<String> PCollectionInput =  p.apply(TextIO.read().from(dataPath));
    PCollection<String> PCollectionLine  =PCollectionInput.apply(Filter.by((String line) -> !line.isEmpty()));
    PCollection<String> pcollectionInputlines=PCollectionLine.apply(Filter.by((String line) -> !line.equals(" ")));
    PCollection<String> pcollectionInputLinkLines=pcollectionInputlines.apply(Filter.by((String line) -> line.startsWith("[")));
   
    PCollection<String> PCollectionInLinks=pcollectionInputLinkLines.apply(
            MapElements.into(TypeDescriptors.strings())
                .via((String pcollectionLinkLine) -> pcollectionLinkLine.
                     substring(pcollectionLinkLine.indexOf("(")+1,pcollectionLinkLine.indexOf(")")) ));

    PCollection<KV<String, String>> PCollectionLinkKVPair=PCollectionInLinks.apply(
                  MapElements.into(  
                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                      .via (link ->  KV.of(dataFile , link) ));
     
                   
    return PCollectionLinkKVPair;
}
}





//PCollection<String> pcolInputLines = 
  //p.apply(TextIO.read().from(dataPath));
     //.apply(Filter.by((String line) -> !line.isEmpty()))
     //.apply(Filter.by((String line) -> !line.equals(" ")))
//PCollection<String> pcolLinkLines = 
   //pcolInputLines .apply(Filter.by((String line) -> line.startsWith("[")))

//PCollection<String> pcolLinks = pcolInputLines .apply(MapElements
//         .into(TypeDescriptors.strings())
//         .via(
//           (String linkline) ->
//             linkline.substring(linkline.indexOf("(")+1, linkline.length()-1)
//                     )
//                   );

// PCollection<String> pcolKVpairsJob1 = pcolInputLines .apply(MapElements
//         .into(TypeDescriptors.kvs.kv(TypeDescriptors.strings(),TypeDescriptors.strings())
//         .via(
//           (String linkline) ->
           
  
              //      )
              //    );
        // Concept #2: Apply a FlatMapElements transform the PCollection of text lines.
        // This transform splits the lines in PCollection<String>, where each element is an
        // individual word in Shakespeare's collected texts.
        // .apply(
           // FlatMapElements.into(TypeDescriptors.strings())
             //   .via((String line) -> Arrays.asList(line.split("[^\\p{L}]+"))))
        // We use a Filter transform to avoid empty word
        //.apply(Filter.by((String word) -> !word.isEmpty()))
        // Concept #3: Apply the Count transform to our PCollection of individual words. The Count
        // transform returns a new PCollection of key/value pairs, where each key represents a
        // unique word in the text. The associated value is the occurrence count for that word.
        //.apply(Count.perElement())
        // Apply a MapElements transform that formats our PCollection of word counts into a
        // printable string, suitable for writing to an output file.
        //.apply(
         //   MapElements.into(TypeDescriptors.strings())
              //  .via(
                 //   (KV<String, Long> wordCount) ->
                     //   wordCount.getKey() + ": " + wordCount.getValue()))
        // Concept #4: Apply a write transform, TextIO.Write, at the end of the pipeline.
        // TextIO.Write writes the contents of a PCollection (in this case, our PCollection of
        // formatted strings) to a series of text files.
        //
//         // By default, it will write to a set of files with names like wordcounts-00001-of-00005
//         pcolLinks .apply(TextIO.write().to("priyaout"));

//     p.run().waitUntilFinish();
//   }
// }
