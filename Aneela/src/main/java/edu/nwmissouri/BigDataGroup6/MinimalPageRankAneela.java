
package edu.nwmissouri.BigDataGroup6;


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

import java.util.Arrays;
import java.util.Collection;
import java.util.ArrayList;
import java.io.File;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;


public class MinimalPageRankAneela {
  // DEFINE DOFNS
  // ==================================================================
  // You can make your pipeline assembly code less verbose by defining
  // your DoFns statically out-of-line.
  // Each DoFn<InputT, OutputT> takes previous output
  // as input of type InputT
  // and transforms it to OutputT.
  // We pass this DoFn to a ParDo in our pipeline.

  /**
   * DoFn Job1Finalizer takes KV(String, String List of outlinks) and transforms
   * the value into our custom RankedPage Value holding the page's rank and list
   * of voters.
   * 
   * The output of the Job1 Finalizer creates the initial input into our
   * iterative Job 2.
   */

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

  static class Job2Updater extends DoFn<KV<String, Iterable<RankedPage>>, KV<String, RankedPage>> {
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


  public static void main(String[] args) {

    PipelineOptions options = PipelineOptionsFactory.create();

    // Create the Pipeline object with the options we defined above
    Pipeline p = Pipeline.create(options);
    String dataFolder = "web04";

    //String dataFile = "go.md";

    //String dataPath = dataFolder + "/" + dataFile;

    //p.apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/kinglear.txt"))

    PCollection<KV<String, String>> pcolKVpair1 = AneelaMap1(p,"go.md",dataFolder);
    PCollection<KV<String, String>> pcolKVpair2 = AneelaMap1(p,"java.md",dataFolder);
    PCollection<KV<String, String>> pcolKVpair3 = AneelaMap1(p,"python.md",dataFolder);
    PCollection<KV<String, String>> pcolKVpair4 = AneelaMap1(p,"README.md",dataFolder);
    
    
   PCollection<KV<String,String>> pcolKVpair5 = AneelaMap1(p,"cSharp.md",dataFolder);

   
    PCollectionList<KV<String, String>> PColKVPairList = PCollectionList.of(pcolKVpair1).and(pcolKVpair2)
        .and(pcolKVpair3).and(pcolKVpair4).and(pcolKVpair5);

    PCollection<KV<String, String>> PCMergeList = PColKVPairList.apply(Flatten.<KV<String, String>>pCollections());
    
    PCollection<KV<String, Iterable<String>>> PCGrpList =PCMergeList.apply(GroupByKey.create());
    
    
    
    PCollection<String> PColLink = PCGrpList.apply(
        MapElements.into(
            TypeDescriptors.strings())
            .via((myMergeLstout) -> myMergeLstout.toString()));
       
        // By default, it will write to a set of files with names like wordcounts-00001-of-00005
        //longLinkLines.apply(TextIO.write().to("pageRankAneela"));
        PColLink.apply(TextIO.write().to("AneelaKVPairsOutput"));

    p.run().waitUntilFinish();
  }

  private static PCollection<KV<String, String>> AneelaMap1(Pipeline p, String dataFile, String dataFolder) {
    String dataPath = dataFolder + "/" + dataFile;

    PCollection<String> PCInput =  p.apply(TextIO.read().from(dataPath));
    PCollection<String> PColLine  =PCInput.apply(Filter.by((String line) -> !line.isEmpty()));
    PCollection<String> pcColInputlines=PColLine.apply(Filter.by((String line) -> !line.equals(" ")));
    PCollection<String> pcolInputLinkLines=pcColInputlines.
                                            apply(Filter.by((String line) -> line.startsWith("[")));
   
    PCollection<String> PColInLinks=pcolInputLinkLines.apply(
            MapElements.into(TypeDescriptors.strings())
                .via((String pcolLinkLine) -> pcolLinkLine.
                     substring(pcolLinkLine.indexOf("(")+1,pcolLinkLine.indexOf(")")) ));

                PCollection<KV<String, String>> PColLinkKVPair=PColInLinks.apply(
                  MapElements.into(  
                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                      .via (link ->  KV.of(dataFile , link) ));
     
                   
    return PColLinkKVPair;
}

 /**
 * Run one iteration of the Job 2 Map-Reduce process
 * Notice how the Input Type to Job 2.
 * Matches the Output Type from Job 2.
 * How important is that for an iterative process?
 * 
 * @param kvReducedPairs - takes a PCollection<KV<String, RankedPage>> with
 *                       initial ranks.
 * @return - returns a PCollection<KV<String, RankedPage>> with updated ranks.
 */
private static PCollection<KV<String, RankedPage>> runJob2Iteration(
  PCollection<KV<String, RankedPage>> kvReducedPairs) {

//    PCollection<KV<String, RankedPage>> mappedKVs = kvReducedPairs.apply(ParDo.of(new Job2Mapper()));

// KV{README.md, README.md, 1.00000, 0, [java.md, 1.00000,1]}
// KV{README.md, README.md, 1.00000, 0, [go.md, 1.00000,1]}
// KV{java.md, java.md, 1.00000, 0, [README.md, 1.00000,3]}

// PCollection<KV<String, Iterable<RankedPage>>> reducedKVs = mappedKVs
//     .apply(GroupByKey.<String, RankedPage>create());

// KV{java.md, [java.md, 1.00000, 0, [README.md, 1.00000,3]]}
// KV{README.md, [README.md, 1.00000, 0, [python.md, 1.00000,1], README.md,
// 1.00000, 0, [java.md, 1.00000,1], README.md, 1.00000, 0, [go.md, 1.00000,1]]}

// PCollection<KV<String, RankedPage>> updatedOutput = reducedKVs.apply(ParDo.of(new Job2Updater()));

// KV{README.md, README.md, 2.70000, 0, [java.md, 1.00000,1, go.md, 1.00000,1,
// python.md, 1.00000,1]}
// KV{python.md, python.md, 0.43333, 0, [README.md, 1.00000,3]}

PCollection<KV<String, RankedPage>> updatedOutput = null;
return updatedOutput;
}

public static  void deleteFiles(){
  final File file = new File("./");
  for (File f : file.listFiles()){
    if(f.getName().startsWith("AneelaAnkam")){
      f.delete();
    }
  }
}



}
