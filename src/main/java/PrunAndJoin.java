import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PrunAndJoin {
    public static class PAJMAP extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filename = fileSplit.getPath().getName();
            String[] extract = value.toString().split("\t");

            if (filename.equals("forum_users.tsv")) {
                if (!extract[0].equals("\"user_ptr_id\"")){
                    context.write(new Text(extract[0].substring(1, extract[0].length() - 1)), new Text("F" + extract[1].substring(1, extract[1].length() - 1)));
                }
                    //context.write(new Text(extract[0].replaceAll("\"","")), new Text("F" + extract[1].replaceAll("\"","")));

            } else {
                if (extract.length == 9 && !extract[0].equals("\"id\"")) {
                    //context.write(new Text(extract[3].replaceAll("\"","")), new Text("T" + extract[8].replaceAll("\"","")));
                    context.write(new Text(extract[3].substring(1, extract[3].length() - 1)), new Text("T" + extract[8].substring(1, extract[8].length() - 1)));
                }
            }
        }
    }

    public static class PAJREDUCE extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> Values, Context context) throws IOException, InterruptedException {
            int count = 0; String result = "";
            for(Text t : Values){
                if (t.toString().substring(0,1).equals("T")){
                    count += Integer.parseInt(t.toString().substring(1));
                }
                else{
                    result = t.toString().substring(1);
                }
            }
            context.write(new Text(key.toString()), new Text(result + "\t" + count ));
        }
    }

    public static void main(String args[]) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Prunning and Joining");

        job.setJarByClass(PrunAndJoin.class);
        job.setMapperClass(PAJMAP.class);
        job.setReducerClass(PAJREDUCE.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
