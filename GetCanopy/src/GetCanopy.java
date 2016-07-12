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


import java.io.*;
import java.util.*;

public class GetCanopy {
    public static class CanopyMapper extends Mapper<Object,Text,Text,Text>{
        Vector<Vector<String>> canopies = new Vector<Vector<String>>();
        public HashMap<String,Integer>counter = new HashMap<>();
        public Integer canopy_num = 0;
        int uthreshold = 8;
        int lthreshold = 2;
        public Vector<String> genRaterIDs(String r,int begin){
            Vector<String>ans = new Vector<>();
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
        public boolean isNewCenter(String r,Vector<Integer> cids){
            cids.clear();
            if(canopy_num ==0) return true;
            else{
                boolean is_new = true;
                int i = r.indexOf("->");
                String movie_id = r.substring(0,i);
                for(int j = 0;j<canopies.size();j++){
                    for(int k = 0;k<canopies.size();k++){
                        if(canopies.get(j).get(k).equals(movie_id)){
                            cids.add(j);
                            is_new = false;
                        }
                    }
                }
                return is_new;
            }
        }
        @Override
        public void map(Object key,Text value,Context context)throws IOException,InterruptedException{
            String r = value.toString();
            Vector<Integer>cids = new Vector<>();
            if(isNewCenter(r,cids)){
                int i = r.indexOf("->");
                Vector<String> rater_ids = genRaterIDs(r,i+2);
                counter.clear();
                for(int j = 0;j<rater_ids.size();j++){
                    String rater_id = rater_ids.get(j);
                    Vector<String> list = invList.get(rater_id);
                    for(int k = 0;k<list.size();k++){
                        String mvid = list.get(k);
                        if(counter.containsKey(mvid)){
                            int num = counter.get(mvid);
                            num++;
                            counter.put(mvid,num);
                        }
                        else{
                            counter.put(mvid,1);
                        }
                    }
                }
                Iterator it = counter.entrySet().iterator();
                Vector<String> canopySet = new Vector<>();
                while(it.hasNext()){
                    Map.Entry entry = (Map.Entry)it.next();
                    String k = (String)entry.getKey();
                    Integer v = (Integer)entry.getValue();
                    if((v>=uthreshold)||(v<uthreshold&&v>=lthreshold)){
                        canopySet.add(k);
                    }
                }
                canopies.add(canopySet);
                r+="|||"+canopy_num.toString();
                canopy_num++;
                value.set(r);
                context.write(value, new Text());
            }
            else{
                r+="|||";
                for(int i = 0;i<cids.size();i++){
                    r+=cids.get(i).toString()+",";
                }
                r=r.substring(0,r.length()-1);
                value.set(r);
                context.write(value,new Text());
            }
        }
    }

//    public static class CanopyReducer extends Reducer<Text,Text,Text,Text>{
//
//    }
    public static HashMap<String,Vector<String>> invList = new HashMap<>();
    public static void main(String args[])throws Exception{
        System.err.println("reading inverted_list....");
        String inPath = "E:\\MovieCluster\\large_data\\large_inverted_data\\large_inverted_data";
        FileReader f = new FileReader(inPath);
        BufferedReader fin = new BufferedReader(f);
        String line;
        StringTokenizer tokenizer;
        while((line = fin.readLine())!=null){
            Vector<String> tmp = new Vector<>();
            tokenizer = new StringTokenizer(line,":,");
            int i = 0;
            String rater_id = new String();
            while(tokenizer.hasMoreTokens()){
                if(i==0){
                    rater_id = tokenizer.nextToken();
                }
                else{
                    tmp.add(tokenizer.nextToken());
                }
                i++;
            }
            invList.put(rater_id,tmp);
        }
        fin.close();
        System.err.println("start mapreduce....");
        inPath = "E:\\MovieCluster\\large_data\\large_reprocessed_data\\large_reprocessed_data";
        String outPath = "E:\\MovieCluster\\large_data\\large_canopy_data";
        System.setProperty("hadoop.home.dir","E:\\share\\yarn\\hadoop-2.7.1");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "build");
        job.setJarByClass(GetCanopy.class);
        job.setMapperClass(CanopyMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job,new Path(inPath));
        FileOutputFormat.setOutputPath(job,new Path(outPath));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
