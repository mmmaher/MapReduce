/*
 * ClickRate.java
 * Megan Maher and Nikki Morin
 * Bowdoin College, Class of 2016
 * Distributed Systems: MapReduce
 *
 * Created: March 30, 2015
 * Last Modified: April 11, 2015
 */

import java.io.IOException;
import java.util.*;
import java.util.Random;

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

/*
 * ClickRate job that pipes input to output as MapReduce-created key-value pairs
 */
public class ClickRate extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ClickRate(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
                System.err.println("Wrong num of parameters");
                System.err.println("Expected: [in] [out]");
                System.exit(1);
        }

        Configuration conf = getConf();

        Job job = new Job(conf, "first admap job");
        job.setJarByClass(ClickRate.class);
        job.setMapperClass(ClickRate.FirstMapper.class);
        job.setReducerClass(ClickRate.FirstReducer.class);
        job.setMapOutputKeyClass(Text.class);

        Job job_2 = new Job(conf, "second admap job");
        job_2.setJarByClass(ClickRate.class);
        job_2.setMapperClass(ClickRate.SecondMapper.class);
        job_2.setReducerClass(ClickRate.SecondReducer.class);
        job_2.setMapOutputKeyClass(Text.class);

        Random random = new Random();
        int ranInt = random.nextInt(100);
        String ranDirName = "/user/input/randir" + Integer.toString(ranInt);

        Path firstOutputPath = new Path(ranDirName);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, firstOutputPath);

        FileInputFormat.addInputPath(job_2, firstOutputPath);
        FileOutputFormat.setOutputPath(job_2, new Path(args[2]));

        return (job.waitForCompletion(true) && job_2.waitForCompletion(true)) ? 0 : 1;
    }

    /*
     * Input: One object of a click or impression file
     * Function: Parses JSON object to extract impression ID, ad ID, referrer name
     * Output: Key = impression ID
     *         Value = 1 or 0 (click or impression), (ad ID, referrer name) -> depending on 1 or 0 value
     */
    public static class FirstMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
            String referrer = "";
            String adid = "";
            String impressionid = "";
            String returnVals = "";

            String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
            String whole = val.toString();

            // Get only the JSON part of the string
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
                referrer = (String) json.get("referrer");
                adid = (String) json.get("adId");
                impressionid = (String) json.get("impressionId");
            } catch (ParseException ex) {
                ex.printStackTrace();
            }
            
            // If there is no referrer, we know it's a click!
            // If click: return 1
            // If impression: return JSON string of referrer and ad ID
            if (referrer.equals("null")) {
                returnVals = "1";
            } else {
                HashMap<String,String> hash = new HashMap<String,String>();
                hash.put("adId", adid);
                hash.put("referrer", referrer);
                JSONObject obj = new JSONObject(hash);
                returnVals = (obj.toJSONString());
            }
            context.write(new Text(impressionid), new Text(returnVals));
        }
    }

    /*
     * Input: Key = impression ID
     *        Value = JSON of referrer and ad ID
     * Output: Key = JSON of referrer and ad ID
     *         Value = 0 or 1 (no click or click)
     */
    public static class FirstReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int clicks = 0;
            boolean flag = false;
            String outKey = "";
            String outVal = "";

            for (Text value : values) {
                String valueString = value.toString();
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

    /*
     * Input: Key = offset number
     *        Value = JSON of referrer and ad ID, numclicks
     * Output: Key = [referrer, adid]
     *         Value = 0 or 1 (no click or click)
     */
    public static class SecondMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
            
            String referrer = "";
            String adid = "";
            String valueNo = "";

            String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
            String whole = val.toString();

            // Get only the json part of the string
            int indexOfFirstCurly = whole.indexOf("{");
            int indexOfSecondCurly = whole.indexOf("}");
            // System.out.println("Whole = " + whole);
            String line = "";
            if (indexOfFirstCurly == -1 || indexOfSecondCurly == -1) {
                line = whole;
            }
            else {
                line = whole.substring(indexOfFirstCurly, indexOfSecondCurly+1);
            }

            valueNo = whole.substring(whole.length() - 1);

            try {
                JSONParser parser = new JSONParser();
                JSONObject json = (JSONObject)parser.parse(line);
                referrer = (String) json.get("referrer");
                adid = (String) json.get("adId");
            } catch (ParseException ex) {
                ex.printStackTrace();
            }

            StringBuilder idreturn =  new StringBuilder();
            idreturn.append("[");
            idreturn.append(referrer);
            idreturn.append(", ");
            idreturn.append(adid);
            idreturn.append("]");

            context.write(new Text(idreturn.toString()), new Text(valueNo));
        }
    }


    /*
     * Input: Key = [referrer, adid]
     *        Value = 0 or 1 (no click or click)
     * Output: Key = [referrer, adid]
     *         Value = ratio of clicks : one occurance of impression
     */
    public static class SecondReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int impressions = 0;
            int clicks = 0;
            double rate = 0.0;

            for (Text value : values) {
                String currVal = value.toString();
                if (Integer.parseInt(currVal) <= 0) {
                    impressions++;
                } else {
                    clicks++;
                }
            }

            if (clicks + impressions > 0) {
                rate = (double)clicks / (double)(clicks + impressions);
            }

            context.write(key, new Text(Double.toString(rate)));
        }
    }
}