
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
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
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
}