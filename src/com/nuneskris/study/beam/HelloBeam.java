package com.nuneskris.study.beam; 

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * batch pipeline.
 * Input file(s)
 * This project can be run locally.
 * should be a text log in the following format:
 * "id","inning","over","ball","batsman","non_striker","bowler","batsman_runs","extra_runs","total_runs","non_boundary","is_wicket","dismissal_kind","player_dismissed","fielder","extras_type","batting_team","bowling_team"
 */
public class HelloBeam {
	
    public static final void main(String args[]) throws Exception {
        processLocal();
     }
    private static void processLocal(){
        Pipeline pipeline = Pipeline.create();

        writeLocally(
           sumUpValuesByKey(
               groupByKeysOfKV(
                   convertToKV(
                       filterWickets(
                           parseMapElementsSplitString(
                               getLocalData(pipeline)))))));

        pipeline.run();
        
    }


    private static PCollection<String> getLocalData(Pipeline pipeline) {
    	PCollection<String> pCollectionExtract = pipeline.apply(TextIO.read().from("/Users/krisnunes/eclipse-workspace/HelloApacheBeam/resources/IPL-Ball-by-Ball 2008-2020.csv"));
    	return pCollectionExtract;
    }
    
    // ******** Function: MapElements *************************
    //MapElements Applies a simple 1-to-1 mapping function over each element in the collection.
    // the SimpleFunction takes a single input and a single output. In our case the input will be a String which represent a row which is not yet split into columns.
    // We will then split this column based on a commma into a array of Strings which will be the next PCollection. So the output willbe a String array (String[])
    private static PCollection<String[]> parseMapElementsSplitString(PCollection<String> input) {
        return
                input.apply(
                        "parse",
                        MapElements.via(
                                new SimpleFunction<String, String[]>() {
									private static final long serialVersionUID = -749614092566773843L;
									@Override
                                    public String[] apply(String input) {
                                        String[] split = input.split(",");
                                        return split;
                                    }
                                }));
    }


    // ******** Function: Filter *************************
    // A very basic operation in many Transformations. Given a filter condition (predicate), filter out all elements that don’t satisfy that predicate. 
    // Can also be used to filter based on an inequality condition. 
    // Essentially, the Filter functions will filter out rows which math the condition true (11th column with value 1)
    private static PCollection<String[]> filterWickets(PCollection<String[]> input) {
        return input.apply(
        			"filterWickets",(
					Filter.by(
							new SerializableFunction<String[], Boolean>() {
									private static final long serialVersionUID = 8934057075773347889L;
									@Override
				                    public Boolean apply(String[] input) {
				                        return input[11].equalsIgnoreCase("1");
				                    }
                })));
    }



    // ******** Class: KV *************************
    // the MapElements for each row, we will take the filtered rows and build a key which is the a single string of the name of the player and the wicket type. 
    // Introducing a comma to separate them which we will use as separate column.
    // To count the each occurrence we will set the value 1 as type Integer.
    // This will create a key <batsman column value>,<dismissal_kind column value>
    // The output will be a key-value of type String and Integer.
    private static PCollection<KV<String, Integer>> convertToKV(PCollection<String[]> filterWickets){
            return filterWickets.apply(
                    "convertToKV",
                    MapElements.via(
                    		new SimpleFunction<String[], KV<String, Integer>>() {
                    				private static final long serialVersionUID = -3410644975277261494L;
									@Override
				                    public KV<String, Integer> apply (String[]input){
				                        String key = input[4] + "," + input[12];
				                    return KV.of(key, Integer.valueOf(1));
                }
                }));
    }
   // ******** Function: GroupByKey *************************
   // This will group by <batsman column value>,<dismissal_kind column value> and the values will be a Iterable of the value which are Integers.
   // 
   private static PCollection<KV<String, Iterable<Integer>>> groupByKeysOfKV(PCollection<KV<String, Integer>>  convertToKV) {
       return convertToKV.apply(
    		   "groupByKey",
                GroupByKey.<String, Integer>create()
        );
    }

   // ******** Function: ParDo *************************
   //we can have a Pardo function inside if we not want to reuse it.
   // context is how we get access to the input (element) and set the output.
    private static PCollection<String> sumUpValuesByKey(PCollection<KV<String, Iterable<Integer>>> kvpCollection){
        return   kvpCollection.apply(
                	"SumUpValuesByKey",
                		ParDo.of(
	                        new DoFn<KV<String, Iterable<Integer>>, String>() {
								private static final long serialVersionUID = -7251428065028346079L;
								@ProcessElement
	                            public void processElement(ProcessContext context) {
	                                Integer totalWickets = 0;
	                                String playerAndWicketType = context.element().getKey();
	                                Iterable<Integer> wickets = context.element().getValue();
	                                for (Integer amount : wickets) {
	                                    totalWickets += amount;
	                                }
	                                context.output(playerAndWicketType + "," + totalWickets);
	                            }
	                        }));

    }

    private static void writeLocally(PCollection<String> sumUpValuesByKey){
        sumUpValuesByKey.apply(TextIO.write().to("/Users/krisnunes/eclipse-workspace/HelloApacheBeam/resources//IPLOuts.csv").withoutSharding());
    }

}