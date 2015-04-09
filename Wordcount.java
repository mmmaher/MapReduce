import java.io.IOException;
import java.util.*;

import java.lang.StringBuilder;

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

// Wordcount job that pipes input to output as MapReduce-created key-value pairs

public class Wordcount extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Wordcount(), args);
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

        Job job = new Job(conf, "wordcount job");
        job.setJarByClass(Wordcount.class);

        job.setMapperClass(Wordcount.IdentityMapper.class);
        job.setReducerClass(Wordcount.IdentityReducer.class);

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
            // Write (key, val) out to memory/disk
            String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
            String line = val.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                    word.set(tokenizer.nextToken());
                    context.write(word, new Text(filename));
            }
        }
	}

	public static class IdentityReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // write (key, val) for every value
            StringBuilder docList = new StringBuilder();
            for (Text value : values) {
                    docList.append(value + " ");
            }

            context.write(key, new Text(docList.toString()));
        }
	}

}
