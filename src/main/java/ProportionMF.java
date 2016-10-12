/**
 * Created by nicolas on 12/10/16.
 */

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class ProportionMF {

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        private IntWritable number_f = new IntWritable();
        private IntWritable number_m = new IntWritable();
        private final static Text word_m = new Text("m");
        private final static Text word_f = new Text("f");

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String val = value.toString();
            String[] line = val.split(";");
            String gen = line[1];
            String[] genders = gen.split(",");


            for(String g : genders ){
                if(g.equals("m")) {
                    number_m.set(1);
                    number_f.set(0);
                }
                else if(g.equals("f")){
                    number_m.set(0);
                    number_f.set(1);
                }

                context.write(word_m, number_m);
                context.write(word_f,number_f);

            }

        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, FloatWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            int size = 0;
            for (IntWritable val : values) {
                sum += val.get();
                size += 1;
            }

            context.write(key, new FloatWritable((float)sum/size));
        }
    }

    public static void main(String[] args) throws Exception {


        Job job = Job.getInstance();
        job.setJarByClass(ProportionMF.class);

        job.setJobName("ProportionMF");


        job.setOutputKeyClass(Text.class);
        /*
            Beware: not the same output value type for map and reduce:
         */
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }

}


