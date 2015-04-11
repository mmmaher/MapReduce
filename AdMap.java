import java.io.IOException;
import java.util.*;

import java.lang.StringBuilder;

import org.json.simple.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.*;

import org.json.simple.*;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;




// AdMap job that pipes input to output as MapReduce-created key-value pairs

public class AdMap extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new AdMap(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
                System.err.println("Wrong num of parameters");
                System.err.println("Expected: [in] [out]");
                System.exit(1);
        }

        Configuration conf = getConf();

        Job job = new Job(conf, "first admap job");
        job.setJarByClass(AdMap.class);
        job.setMapperClass(AdMap.FirstMapper.class);
        job.setReducerClass(AdMap.FirstReducer.class);
        job.setMapOutputKeyClass(Text.class);

        Job job_2 = new Job(conf, "second admap job");
        job_2.setJarByClass(AdMap.class);
        job_2.setMapperClass(AdMap.SecondMapper.class);
        job_2.setReducerClass(AdMap.SecondReducer.class);
        job_2.setMapOutputKeyClass(Text.class);

        // String firstOutputPath = "/user/root/first_output"

        // FileInputFormat.addInputPath(job, new Path(args[0]));
        // FileInputFormat.addInputPath(job, new Path(args[1]));
        // FileOutputFormat.setOutputPath(job, new Path(args[2]));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job_2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job_2, new Path(args[1]));

        return (job.waitForCompletion(true) && job_2.waitForCompletion(true)) ? 0 : 1;
    }

    public static class FirstMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
            // Isolate the impression_id, pass that as the key; the value should be
            // a click or an impression
            // ignore the key, will always be zero
            // the val is the long list
            // want the ad_id, referrer, and the click rate
            // so pass key == impression id, the ad_id, the referrer, and whether click or impression

            String referrer = "";
            String adid = "";
            String impressionid = "";
            String returnVals = "";

            String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
            String whole = val.toString();

            // Get only the json part of the string
            int indexOfCurly = whole.indexOf("{");
            System.out.println("Whole = " + whole);
            String line = "";
            if (indexOfCurly == -1) {
                line = whole;
            }
            else {
                line = whole.substring(indexOfCurly);
            }
            
            try {
                JSONParser parser = new JSONParser();
                JSONObject json = (JSONObject)parser.parse(line);
                referrer = referrer + (String) json.get("referrer");
                adid = adid + (String) json.get("adId");
                impressionid = impressionid + (String) json.get("impressionId");
            } catch (ParseException ex) {
                ex.printStackTrace();
            }
            
            // If there is no referrer, we know it's a click!
            // If click: return 1
            // If impression: return json string of referrer and adId
            if (referrer.equals("null")) {
                returnVals = "1";
            } else {
                // returnVals = referrer + " " + adid;
                HashMap<String,String> hash = new HashMap<String,String>();
                hash.put("adId", adid);
                hash.put("referrer", referrer);
                JSONObject obj = new JSONObject(hash);
                returnVals = (obj.toJSONString());
            }
            // System.out.println("Returnvals = " + returnVals);

            context.write(new Text(impressionid), new Text(returnVals));
        }
    }

    public static class FirstReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // input:
            // key = impressionid
            // value = json of referrer and adid
            // output:
            // key = json of referrer and adid
            // num clicks

            int clicks = 0;
            boolean flag = false;
            String outKey = "";
            String outVal = "";

            for (Text value : values) {
                String valueString = value.toString();
                // System.out.println("Value = " + valueString);
                if (valueString.contains("referrer")) {
                    System.out.println("contains referrer!");
                    outKey = valueString;
                }

                if (valueString.equals("1")) {
                    clicks++;
                }
            }
            outVal = Integer.toString(clicks);
            context.write(new Text(outKey), new Text(outVal));
        }
    }

// _________________________________NEW MAP AND REDUCE FUNCS BELOW___________________________________________
// _________________________________NEW MAP AND REDUCE FUNCS BELOW___________________________________________


    public static class SecondMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
            



            // Isolate the impression_id, pass that as the key; the value should be
            // a click or an impression
            // ignore the key, will always be zero
            // the val is the long list
            // want the ad_id, referrer, and the click rate
            // so pass key == impression id, the ad_id, the referrer, and whether click or impression

            // String referrer = "";
            // String adid = "";
            // String impressionid = "";
            // String clickOrImpression = "";
            // String returnVals = "";

            // String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
            // String whole = val.toString();
            // int indexOfCurly = whole.indexOf("{");

            // System.out.println("Whole = " + whole);
            // String line = "";
            // if (indexOfCurly == -1) {
            //     line = whole;
            // }
            // else {
            //     line = whole.substring(indexOfCurly);
            // }

            // // System.out.println("here now");
            // Object entry;
            // try {

            //     JSONParser parser = new JSONParser();
            //     JSONObject json = (JSONObject)parser.parse(line);
            //     referrer = referrer + (String) json.get("referrer");
            //     adid = adid + (String) json.get("adId");
            //     impressionid = impressionid + (String) json.get("impressionId");

            // } catch (ParseException ex) {
            //     ex.printStackTrace();
            // }
            
            // // if there is no referrer, we know it's a click!
            // if (referrer.equals("null")) {
            //     clickOrImpression = clickOrImpression + "click";
            //     returnVals = clickOrImpression + " " + adid;
            // } else {
            //     clickOrImpression = clickOrImpression + "impression";
            //     returnVals = clickOrImpression + " " + adid + " " + referrer;
            // }

            context.write(new Text(key.toString() + "aaa"), val);

        }
    }

    public static class SecondReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // int impressions = 0;
            // int clicks = 0;
            // double rate = 0.0;

            // for (Text value : values) {
            //     String currVal = value.toString();
            //     if (Integer.parseInt(currVal) <= 0) {
            //         impressions++;
            //     } else {
            //         clicks++;
            //     }
            // }

            // if (clicks + impressions > 0) {
            //     rate = (double)clicks / (double)(clicks + impressions);
            // }

            // context.write(key, new Text(Double.toString(rate) + " yo"));


            for (Text value : values) {
                context.write(key, value);
            }




            // double impressions = 0;
            // double clicks = 0;
            // boolean adflag = false;
            // boolean referrerflag = false;
            // String adid = "";
            // String referrer = "";

            // System.out.println("Here");
            // System.out.println("Key = " + key);

            // for (Text value : values) {
            //     adid = value.toString();
            // }

            // for (Text value : values) {
            //     System.out.println("Value = " + value);
            //     String line = value.toString();
            //     String[] data = line.split(" ");

            //     if (data.length > 2) {
            //         if (!adflag) { adid = adid + data[1]; adflag = true; }

            //         if (data[0].equals("click")) {
            //             clicks++;
            //         } else {
            //             impressions++;
            //             if (!referrerflag) {
            //                 referrer = referrer + data[2];
            //                 referrerflag = true;
            //             }
            //         }
            //     } else { System.out.println("Data too short"); }

            // }

            // String keyString = "[" + referrer + ", " + adid + "]";
            // double rate = 0.0;
            // if (clicks + impressions > 0) {
            //     rate = clicks / (clicks + impressions);
            // }
            // else { System.out.println("ERROR tried to divideby zero");}
            // System.out.println("Clicks: " + clicks);
            // System.out.println("Impressiosns: " + impressions);

            // context.write(new Text(keyString), new Text(Double.toString(rate)));
            // context.write(key, new Text(adid));
            // System.out.println("\n\n");
        }
    }











}