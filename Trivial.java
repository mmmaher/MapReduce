import java.io.IOException;
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

/**
 * Trivial MapReduce job that pipes input to output as MapReduce-created key-value pairs.
 */
public class Trivial extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Trivial(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Error: Wrong number of parameters");
            System.err.println("Expected: [in] [out]");
            System.exit(1);
        }

        Configuration conf = getConf();

        Job job = new Job(conf, "trivial job");
        job.setJarByClass(Trivial.class);

        job.setMapperClass(Trivial.IdentityMapper.class);
        job.setReducerClass(Trivial.IdentityReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * map: (LongWritable, Text) --> (LongWritable, Text)
     * NOTE: Keys must implement WritableComparable, values must implement Writable
     */
    public static class IdentityMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        @Override
        public void map(LongWritable key, Text val, Context context)
                throws IOException, InterruptedException {
            // write (key, val) out to memory/disk
            context.write(key, val);
        }

    }

    /**
     * reduce: (LongWritable, Text) --> (LongWritable, Text)
     */
    public static class IdentityReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            // write (key, val) for every value
            for (Text val : values) {
                context.write(key, val);
            }
        }

    }

}