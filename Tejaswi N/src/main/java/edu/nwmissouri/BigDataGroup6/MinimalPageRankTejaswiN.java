package edu.nwmissouri.BigDataGroup6;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;


public class MinimalPageRankTejaswiN {
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



  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.create();

    Pipeline p = Pipeline.create(options);
    String dataFolder = "web04";
   
   
   PCollection<KV<String,String>> pCol1 = TejuMapper1(p,"go.md",dataFolder);
   PCollection<KV<String,String>> pCol2 = TejuMapper1(p,"python.md",dataFolder);
   PCollection<KV<String,String>> pCol3 = TejuMapper1(p,"java.md",dataFolder);
   PCollection<KV<String,String>> pCol4 = TejuMapper1(p,"README.md",dataFolder);
   PCollection<KV<String,String>> pCol5 = TejuMapper1(p,"Flask.md",dataFolder);
   
    PCollectionList<KV<String, String>> pCollectionList = PCollectionList.of(pCol1).and(pCol2).and(pCol3).and(pCol4).and(pCol5);
   
    PCollection<KV<String, String>> mergedPcol = pCollectionList.apply(Flatten.<KV<String,String>>pCollections());

     PCollection<KV<String, Iterable<String>>> PCGrpList =mergedPcol.apply(GroupByKey.create());
     PCollection<String> PColLink = PCGrpList.apply(
         MapElements.into(
            TypeDescriptors.strings())
            .via((myMergeLstout) -> myMergeLstout.toString()));
      
   
    // PCollection<String> mergedString = mergedPcol.apply(MapElements.into(TypeDescriptors.strings()).via((mergeOutput)->mergeOutput.toString()));
    PColLink.apply(TextIO.write().to("TejuNallavolu_Out"));  
    p.run().waitUntilFinish();
  }

  public static PCollection<KV<String,String>> TejuMapper1(Pipeline p, String dataFile, String dataFolder){
   
    String newdataPath = dataFolder + "/" + dataFile;
     PCollection<String> pcInput = p.apply(TextIO.read().from(newdataPath));
     PCollection<String> pclinkLines = pcInput.apply(Filter.by((String line) -> line.startsWith("[")));
     PCollection<String> pcLinks = pclinkLines.apply(MapElements.into((TypeDescriptors.strings()))
     .via((String linkLine) ->linkLine.substring(linkLine.indexOf("(")+1, linkLine.length()-1)));
    
     PCollection<KV<String,String>> pcKVPairs =  pcLinks.apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
     .via((String outLink) -> KV.of(dataFile,outLink)));
    return pcKVPairs;
  }

}
