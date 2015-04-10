import java.io.IOException;
import java.util.*;

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

        Job job = new Job(conf, "admap job");
        job.setJarByClass(AdMap.class);

        job.setMapperClass(AdMap.IdentityMapper.class);
        job.setReducerClass(AdMap.IdentityReducer.class);

        job.setMapOutputKeyClass(Text.class);

        // job.setInputFormatClass(TextInputFormat.class);
        // job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

	public static class IdentityMapper extends Mapper<LongWritable, Text, Text, Text> {
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
            boolean isImpression = false;
            String valsToReturn;

            String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
            String line = val.toString();
            StringTokenizer tokenizer = new StringTokenizer(line, ":,");
            if (line.contains("referrer")) {
                isImpression = true;
            }

            while (tokenizer.hasMoreTokens()) {
                    if (tokenizer.nextToken().contains("referrer")) {
                        referrer += tokenizer.nextToken();
                    }
                    else if (tokenizer.nextToken().contains("impressionId")) {
                        impresionid += tokenizer.nextToken();
                    }
                    else if (tokenizer.nextToken().contains("adId")) {
                        adid += tokenizer.nextToken();
                    }
            }

            if (isImpression) {
                valsToReturn = "impression " + adid + " " + referrer
            } else {
                valsToReturn = "click " + adid;
            }

            context.write(new Text(impresionid), new Text(valsToReturn));

        }
	}

	public static class IdentityReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // write (key, val) for every value
            // String docList = "";
            for (Text value : values) {
                context.write(key, value);
            }

            // context.write(key, new Text(docList));

        }
	}

}