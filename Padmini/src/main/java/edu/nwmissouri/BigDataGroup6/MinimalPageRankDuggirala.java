
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
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

public class MinimalPageRankDuggirala {

  public static void main(String[] args) {

   
    PipelineOptions options = PipelineOptionsFactory.create();

    // In order to run your pipeline, you need to make following runner specific changes:
    //
   
    // Create the Pipeline object with the options we defined above
    Pipeline p = Pipeline.create(options);


    String dataFolder = "web04";
    // String dataFile = "go.md";
   //  String dataPath = dataFolder + "/" + dataFile;
    //p.apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/kinglear.txt"))
    PCollection<KV<String, String>> duggiralaPCKeyValuePairs1 = PadminiDMapper01(p,"go.md",dataFolder);
    PCollection<KV<String, String>> duggiralaPCKeyValuePairs2 = PadminiDMapper01(p,"java.md",dataFolder);
    PCollection<KV<String, String>> duggiralaPCKeyValuePairs3 = PadminiDMapper01(p,"python.md",dataFolder);
    PCollection<KV<String, String>> duggiralaPCKeyValuePairs4 = PadminiDMapper01(p,"README.md",dataFolder);
    PCollection<KV<String, String>> duggiralaPCKeyValuePairs5 = PadminiDMapper01(p,"JavaScript.md",dataFolder);
 

    PCollectionList<KV<String, String>> pCollectionpair = PCollectionList.of(duggiralaPCKeyValuePairs1).and(duggiralaPCKeyValuePairs2).and(duggiralaPCKeyValuePairs3).
                                and(duggiralaPCKeyValuePairs4).and(duggiralaPCKeyValuePairs5);

    PCollection<KV<String, String>> Mergedl = pCollectionpair.apply(Flatten.<KV<String,String>>pCollections());
    PCollection<KV<String, Iterable<String>>> Merged2 =Mergedl.apply(GroupByKey.create());
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
}