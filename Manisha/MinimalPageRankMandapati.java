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
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

public class MinimalPageRankMandapati {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);
    String dataFolder = "web04";

    PCollection<KV<String, String>> collectionkv1 = MandapatiMap(p, "go.md", dataFolder);
    PCollection<KV<String, String>> collectionkv2 = MandapatiMap(p, "java.md", dataFolder);
    PCollection<KV<String, String>> collectionkv3 = MandapatiMap(p, "python.md", dataFolder);
    PCollection<KV<String, String>> collectionkv4 = MandapatiMap(p, "ReadMe.md", dataFolder);


    PCollectionList<KV<String, String>> result = PCollectionList.of(collectionkv1).and(collectionkv2).and(collectionkv3).and(collectionkv4);
    PCollection<KV<String, String>> mergedList = result.apply(Flatten.<KV<String,String>>pCollections());
    PCollection<String> pckvStrings =  mergedList.apply(
      MapElements.into(  
        TypeDescriptors.strings())
          .via((resultMergeLstout) -> resultMergeLstout.toString()));
             pckvStrings.apply(TextIO.write().to("Mandapaticounts"));
        p.run().waitUntilFinish();
  }

 private static PCollection<KV<String, String>> MandapatiMap(Pipeline p, String dataFile, String Path) {

    String datap = Path + "/" +  dataFile;
    PCollection<String> PCILines =  p.apply(TextIO.read().from(datap));
    PCollection<String> PCLines  = PCILines.apply(Filter.by((String line) -> !line.isEmpty()));
    PCollection<String> PCIEmptyLines = PCLines.apply(Filter.by((String line) -> !line.equals(" ")));
    PCollection<String> PCILLines = PCIEmptyLines.apply(Filter.by((String line) -> line.startsWith("[")));
   
    PCollection<String> PCLinks = PCILLines.apply(
            MapElements.into(TypeDescriptors.strings())
                .via((String linelink) -> linelink.substring(linelink.indexOf("(")+1,linelink.indexOf(")")) ));

                PCollection<KV<String, String>> collKVpairs = PCLinks.apply(
                  MapElements.into(  
                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                      .via (linelink ->  KV.of(dataFile , linelink) ));              
    return collKVpairs;
  }
}
