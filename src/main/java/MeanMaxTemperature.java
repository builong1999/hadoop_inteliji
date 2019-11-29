import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MeanMaxTemperature {
    public static class MeanMap extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] extract = value.toString().split("[,\\t\\r\\n ]");
            context.write(new Text(extract[0].substring(2)), new Text(extract[2]));
        }
    }
    public static class MeanReduce extends   Reducer<Text,Text,Text, DoubleWritable> {
        private DoubleWritable average = new DoubleWritable();
        public void reduce(Text key, Iterable<Text> Values, Context context) throws IOException, InterruptedException{
            int count  = 0;
            Double vals = 0d;
            for (Text data : Values) {
                vals += Double.parseDouble(data.toString().trim());
                count++;
            }
            average.set(vals/count);
            context.write(key,average);
        }
    }
    public static void main(String args[]) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MeanMAxTemperatures");

        job.setJarByClass(MeanMaxTemperature.class);
        job.setMapperClass(MeanMap.class);
        job.setReducerClass(MeanReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}