/**
 * Created by ZhouDavid on 2016/7/12.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.DataOutputStream;
import java.io.IOException;

import java.util.StringTokenizer;
public class PreProcess {
    public static class PreMapper extends Mapper<Object,Text,Text,Text>{
        private String rater_id = new String();
        private Text movie_id = new Text();
        private String rate = new String();
        private Text rating = new Text();
        @Override
        public void map(Object key,Text value,Context context)throws IOException, InterruptedException{
            StringTokenizer tokenizer = new StringTokenizer(value.toString(),",");
            int i =0 ;
            while(tokenizer.hasMoreTokens()){
                if(i>2) break;
                if(i==0){
                    movie_id.set(tokenizer.nextToken());
                }
                else if(i==1){
                    rater_id = tokenizer.nextToken();
                }
                else if(i==2){
                    rate=tokenizer.nextToken();
                }
                i=i+1;
            }
            rating.set(rater_id+":"+rate);
            context.write(movie_id,rating);
        }
    }
    public static class PreReducer extends Reducer<Text,Text,Text,Text>{
        private Text tratings = new Text();
        private String ratings=  new String();
        public void reduce(Text movie_id,Iterable<Text> values, Context context)throws IOException, InterruptedException{
            ratings = "";
            for(Text val:values){
                String rating = val.toString();
                ratings = ratings+rating+",";
            }
            ratings = ratings.substring(0,ratings.length()-1);
            tratings.set(ratings);
            context.write(movie_id,tratings);
        }
    }
    public static class MyOutputFormat extends FileOutputFormat<Text,Text>{
        @Override
        public RecordWriter<Text,Text>getRecordWriter(TaskAttemptContext job)
                throws IOException,InterruptedException{
            Configuration conf = job.getConfiguration();
            Path path=getDefaultWorkFile(job,"");
            FileSystem fs = path.getFileSystem(conf);
            FSDataOutputStream fout = fs.create(path,false);
            return new MyWriter(fout);
        }
    }
    public static class MyWriter extends RecordWriter<Text,Text> {
        protected DataOutputStream out;
        private String KeyValueSep;
        public static final String NEW_LINE = "\r\n";

        public MyWriter(DataOutputStream out, String KeyValueSep){
            this.out = out;
            this.KeyValueSep = KeyValueSep;
        }
        public MyWriter(DataOutputStream out){
            this(out,"->");
        }

        @Override
        public void write(Text title,Text link_list)throws IOException,InterruptedException{
            if(title!=null){
                out.write(title.toString().getBytes());
                out.write(this.KeyValueSep.getBytes());
            }
            if(link_list!=null){
                out.write(link_list.getBytes(),0,link_list.getLength());
                out.write(NEW_LINE.getBytes());
            }
        }

        @Override
        public void close(TaskAttemptContext context)throws IOException,InterruptedException{
            out.close();
        }
    }
    public static void main(String args[])throws Exception{
        System.err.println("start...");
        System.setProperty("hadoop.home.dir","E:\\share\\yarn\\hadoop-2.7.1");
        Configuration conf = new Configuration();
        String inPath = "E:\\MovieCluster\\large_data\\large_processed_data";
        String outPath = "E:\\MovieCluster\\large_data\\large_reprocessed_data";
        Job job = Job.getInstance(conf, "build");
        job.setJarByClass(PreProcess.class);
        job.setMapperClass(PreMapper.class);
        job.setReducerClass(PreReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        job.setOutputFormatClass(MyOutputFormat.class);
        FileInputFormat.addInputPath(job,new Path(inPath));
        FileOutputFormat.setOutputPath(job,new Path(outPath));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
