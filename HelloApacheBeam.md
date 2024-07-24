---
title: "Apache Beam Model. Clean and Simple"
collection: teaching
type: "Data Processing"
permalink: /teaching/HelloApacheBeam
date: 2024-06-01
venue: "Beam"
date: 2024-06-01
location: "Local"
---

I was reviewing a decision on utilizing GCP DataFusion versus DataFlow for simple ETL jobs. The rationale given was the complexity of DataFlow and the difficulty in finding resources with the necessary skills.

DataFlow is based on the open-source Apache Beam project, which allows for defining both batch and streaming pipelines within the same model. My plan is to develop a system using this model, starting with running Apache Beam locally and then progressing to running DataFlow on GCP, all using the same use case. I will be resurrecting a codebase I developed previously to experiment with DataFlow.

# Demo

This project can be run locally. The screen shots are attached.

<img width="612" alt="image" src="/images/teachings/beam/CricketDatascreenshot.png">

The data I will be using is from IPL cricket data with the following columns. The objective is to demonstrate the ease of quickly developing using the Apache Beam model.

"id","inning","over","ball","batsman","non_striker","bowler","batsman_runs","extra_runs","total_runs","non_boundary","is_wicket","dismissal_kind","player_dismissed","fielder","extras_type","batting_team","bowling_team"

## Pipeline
		
A pipeline is a graph (series) of transformations applied to data. Data, which typically consists of multiple rows and columns, is called a Collection, and within the context of Apache Beam, it is called a PCollection.

We will use this pipeline to extract data (PCollection) from a data source (locally in our case), perform a series of transformations on the data (PCollection), and load it into a target (locally).

The pipeline object can be created very simply.

	Pipeline pipeline = Pipeline.create();

### Data Extraction
Apache Beam has multiple I/O integrations. For our local example, we will be using the TextIO class. This will return the data in the CSV file as a PCollection, with each row as a single string.

```java 
    private static PCollection<String> getLocalData(Pipeline pipeline) {
    		PCollection<String> pCollectionExtract = pipeline.apply(TextIO.read().from("/Users/xxxxx/eclipse-workspace/HelloApacheBeam/resources/IPL-Ball-by-Ball 2008-2020.csv"));
    	return pCollectionExtract;
    }
```
### Element wise Transformation: Parsing Extracted Data
The PCollection we currently have is a single string. We need to split this string into individual columns. For this, Beam provides a MapElements transform, which uses the SimpleFunction class that we can customize based on our requirements.

```java
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

```

### Element wise Transformation: Filtering Data
	
In our example, we are only concerned with players who have been dismissed. This is indicated by the 11th column (index 10), where a value of 1 signifies a wicket. We will filter to retain only those rows where the value in the 11th column is 1. The Filter transform will pass through only the rows that meet this condition.

```java
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
```

### Aggregation Transformation: MapElements as Key Values and Perform GroupBy
To perform any aggregation or transformation, we typically need to use a key and then perform aggregations, such as grouping by that key. This approach is commonly used to change the granularity in fact tables.

In our example, we will group by the player's name (column index 4) and the manner of their dismissal (column index 12). We'll count the number of times each player has been dismissed in that manner by attaching a counter of 1 to each occurrence and then summing them up.

```java
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
 ```

### Pardo DoFn
Previously, we used SimpleFunctions for straightforward transformations. For more complex operations, we will use ParDo with DoFn, which can handle side inputs and outputs. Although we can achieve this specific task with a SimpleFunction, we'll use ParDo to add all integer values together as a demonstration.

```java 
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
```

### Load Data
Just like the way we used TextIO to read data. We will be use the same class to write data.

```java
    private static void writeLocally(PCollection<String> sumUpValuesByKey){
        sumUpValuesByKey.apply(TextIO.write().to("/Users/xxxx/eclipse-workspace/HelloApacheBeam/resources//IPLOuts.csv").withoutSharding());
    }

```

## Running the Pipeline
Call the various functions on the pipeline and finally call pipeline.

```java
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
    
 ```
<img width="612" alt="image" src="/images/teachings/beam/CreicketBeamOutput.png">
 
## Build
Deployment XML

```xml
 <dependencies>
        <dependency>
            <groupId>org.apache.beam</groupId>
            <artifactId>beam-sdks-java-core</artifactId>
            <version>2.57.0</version>
        </dependency>
        
        <dependency>
            <groupId>org.apache.beam</groupId>
            <artifactId>beam-runners-direct-java</artifactId>
            <version>2.57.0</version>
        </dependency>

    </dependencies>
```
