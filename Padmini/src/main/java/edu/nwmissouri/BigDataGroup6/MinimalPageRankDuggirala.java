
package edu.nwmissouri.BigDataGroup6;

import java.util.ArrayList;




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
public class MinimalPageRankDuggirala {
  
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
    deleteFiles();

   
    PipelineOptions options = PipelineOptionsFactory.create();

    // In order to run your pipeline, you need to make following runner specific changes:
    //
   
    // Create the Pipeline object with the options we defined above
    Pipeline p = Pipeline.create(options);


    String dataFolder = "web04";
    
    PCollection<KV<String, String>> duggiralaPCKeyValuePairs1 = PadminiDMapper01(p,"go.md",dataFolder);
    PCollection<KV<String, String>> duggiralaPCKeyValuePairs2 = PadminiDMapper01(p,"java.md",dataFolder);
    PCollection<KV<String, String>> duggiralaPCKeyValuePairs3 = PadminiDMapper01(p,"python.md",dataFolder);
    PCollection<KV<String, String>> duggiralaPCKeyValuePairs4 = PadminiDMapper01(p,"README.md",dataFolder);
    PCollection<KV<String, String>> duggiralaPCKeyValuePairs5 = PadminiDMapper01(p,"JavaScript.md",dataFolder);
 

    PCollectionList<KV<String, String>> pCollectionpair = PCollectionList.of(duggiralaPCKeyValuePairs1).and(duggiralaPCKeyValuePairs2).and(duggiralaPCKeyValuePairs3).
                                and(duggiralaPCKeyValuePairs4).and(duggiralaPCKeyValuePairs5);

    PCollection<KV<String, String>> Mergedl = pCollectionpair.apply(Flatten.<KV<String,String>>pCollections());
    PCollection<KV<String, Iterable<String>>> Merged2 =Mergedl.apply(GroupByKey.create());
    PCollection<KV<String, RankedPage>> job2in = Merged2.apply(ParDo.of(new Job1Finalizer()));


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
 
    PCollection<String> PCollString =  Merged2.apply(
      MapElements.into(  
        TypeDescriptors.strings())
          .via((MergeDlisto) -> MergeDlisto.toString()));

       
        //
        // By default, it will write to a set of files with names like wordcounts-00001-of-00005
        PCollString.apply(TextIO.write().to("PadminiDKeyValuePairs"));
       

        p.run().waitUntilFinish();
  }

  private static PCollection<KV<String, String>> PadminiDMapper01(Pipeline p, String dataFile, String dataFolder) {
    String dataPath = dataFolder + "/" + dataFile;

    PCollection<String> pcolInLines =  p.apply(TextIO.read().from(dataPath));
    PCollection<String> PColLine  =pcolInLines.apply(Filter.by((String line) -> !line.isEmpty()));
    PCollection<String> pcColInEmptyLines=PColLine.apply(Filter.by((String line) -> !line.equals(" ")));
    PCollection<String> pcolIninks=pcColInEmptyLines.apply(Filter.by((String line) -> line.startsWith("[")));
   
    PCollection<String> pcolInLinks=pcolIninks.apply(
            MapElements.into(TypeDescriptors.strings())
                .via((String linkline) -> linkline.substring(linkline.indexOf("(")+1,linkline.indexOf(")")) ));

                PCollection<KV<String, String>> PColKV=pcolInLinks.apply(
                  MapElements.into(  
                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                      .via (linkline ->  KV.of(dataFile , linkline) ));
     
                   
    return PColKV;
  }
 



public static  void deleteFiles(){
  final File file = new File("./");
  for (File f : file.listFiles()){
    if(f.getName().startsWith("Padmini")){
      f.delete();
    }
  }
}
}