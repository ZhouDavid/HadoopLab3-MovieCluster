/**
 * Created by ZhouDavid on 2016/7/12.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.hash.Hash;

import java.util.Map;
import java.util.Map.Entry;
import java.io.*;
import java.util.*;


public class GetCanopyCenter {
    public static Integer canopy_num = 0;
    public static Vector<HashSet<String>> canopyCenters = new Vector<HashSet<String>>();

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

    public static class CanopyMapper extends Mapper<Object,Text,Text,Text>{
        int uthreshold = 8;
        public HashSet<String>genRaterIDs(String r,int begin){
            HashSet<String>ans = new HashSet<>();
            int start = begin;
            int end = -1;
            while((end = r.indexOf(":",start))!=-1){
                ans.add(new String(r.substring(start,end)));
                start = r.indexOf(",",end);
                if(start==-1) break;
                start++;
            }
            return ans;
        }

        public static int calSimilarity(HashSet<String> s1,HashSet<String>s2){
            int count =0;
            for(Iterator it = s1.iterator();it.hasNext();){
                if(s2.contains(it.next())){
                    count++;
                }
            }
            return count;
        }

        @Override
        public void map(Object key,Text value,Context context)throws IOException,InterruptedException{
            String line = value.toString();
            int start = line.indexOf('-');
            start+=2;
            HashSet<String> rids = genRaterIDs(line,start);
            if(canopyCenters.size()==0){
                canopyCenters.add(rids);
                context.write(new Text(canopy_num.toString()),new Text(line.substring(start)));
                canopy_num++;
            }
            else{
                boolean stronglyMark = false;
                for(Integer i = 0;i<canopyCenters.size();i++){
                    int share = calSimilarity(rids,canopyCenters.get(i));
                    if(share>=uthreshold){
                        context.write(new Text(i.toString()),new Text(line.substring(start)));
                        stronglyMark = true;
                        break;
                    }
                }
                if(stronglyMark == false){
                    canopyCenters.add(rids);
                    context.write(new Text(canopy_num.toString()),new Text(line.substring(start)));
                    canopy_num++;
                }
            }
        }
    }

    public static class CanopyReducer extends Reducer<Text,Text,Text,Text>{
        public HashMap<String,Double>sum=new HashMap<>();
        @Override
        public void setup(Context context){
            System.err.println(canopy_num);
        }
        public List cutDimension(HashMap<String,Double>sum){
            List <Map.Entry<String,Double>> list = new ArrayList<>(sum.entrySet());
            Collections.sort(list,new Comparator<Map.Entry<String,Double>>(){
                @Override
                public int compare(Entry<String,Double>o1,Entry<String,Double>o2){
                    return o2.getValue().compareTo(o1.getValue());
                }
            });
            return list;
        }

        public String vec2String(List<Map.Entry<String,Double>>list){
            String ans = new String();
            int size = list.size()<20000?list.size():20000;
            System.err.println("dimension_size="+size);
            for(int i = 0;i<size;i++){
                if(i%10000==1)System.err.println(i);
                ans+=list.get(i).getKey()+":"+list.get(i).getValue()+",";
            }
            ans = ans.substring(0,ans.length()-1);
            return ans;
        }
        @Override
        public void reduce(Text key,Iterable<Text>values,Context context)throws IOException, InterruptedException{
            System.err.println("reducing.....");
            double count = 0;
            sum.clear();
            for(Text val:values){
                count++;
                String vec = val.toString();
                int start = 0;
                int end = -1;
                while((end = vec.indexOf(":",start))!=-1){
                    String rid = vec.substring(start,end);
                    start = end+1;
                    end = vec.indexOf(",",start);
                    String rate;
                    boolean is_end = false;
                    if(end ==-1) {
                        is_end = true;
                        rate= vec.substring(start);
                    }
                    else rate = vec.substring(start,end);
                    if(sum.containsKey(rid)){
                        Double s = sum.get(rid);
                        Double r = new Double(0);
                        try{
                            r = Double.parseDouble(rate);
                        }
                        catch(Exception e){
                            String rrid = rid;
                            String rr = rate;
                            System.err.println("rid="+rid+" rate="+rate);
                            int m =1;
                        }
                        s+=r;
                        sum.put(rid,s);
                    }
                    else{
                        sum.put(rid,Double.parseDouble(rate));
                    }
                    if(is_end) break;
                    start=end+1;
                }
            }
            Iterator it= sum.entrySet().iterator();
            while(it.hasNext()){
                Map.Entry entry = (Map.Entry) it.next();
                String k = (String)entry.getKey();
                Double s = sum.get(k);
                s/=count;
                sum.put(k,s);
            }
            //降维
            List list = cutDimension(sum);

            String vect = vec2String(list);
            Text vec = new Text(vect);
            context.write(key,vec);
        }
    }
    public static void main(String args[])throws Exception{
        System.err.println("reading inverted_list....");
        System.err.println("start mapreduce....");
        String inPath = "E:\\HadoopLab3-MovieCluster\\large_data\\large_reprocessed_data\\large_reprocessed_data";
        String outPath = "E:\\HadoopLab3-MovieCluster\\large_data\\large_canopyCenter_data";
        System.setProperty("hadoop.home.dir","E:\\share\\yarn\\hadoop-2.7.1");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "get");
        job.setJarByClass(GetCanopyCenter.class);
        job.setMapperClass(CanopyMapper.class);
        job.setReducerClass(CanopyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(MyOutputFormat.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job,new Path(inPath));
        FileOutputFormat.setOutputPath(job,new Path(outPath));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
