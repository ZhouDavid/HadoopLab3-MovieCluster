/**
 * Created by Administrator on 2016/7/17.
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

import static org.apache.commons.math3.util.FastMath.sqrt;

public class KMeansReducer extends Reducer<Text,Text,Text,Text>{

    public HashMap<String,Double> sum = new HashMap<>();
    public HashMap<String,HashMap<String,Double>> canopyCenter = new HashMap<>();

    public String getCid(String line){
        int end = line.indexOf("-");
        return line.substring(0,end);
    }

    public HashMap<String,Double> getVec(String line){
        HashMap<String,Double> vec = new HashMap<>();
        int start = line.indexOf(">"); start++;
        String v = line.substring(start);
        int end = -1;
        while((end=v.indexOf(":",start))!=-1){
            String rid = v.substring(start,end);
            start = end+1;
            end = v.indexOf(",",start);
            Double rate = null;
            if(end == -1){
                rate = Double.parseDouble(v.substring(start));
            }
            else{
                rate = Double.parseDouble(v.substring(start,end));
            }
            vec.put(rid,rate);
        }
        return vec;
    }

    public Vector<String> updateCanopyCenter(){
        Vector<String> centers = new Vector<>();
        Iterator it = canopyCenter.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry entry = (Map.Entry)it.next();
            int share = calSimilarity(sum,(HashMap<String,Double>)entry.getValue());
            if(share>=2){
                centers.add((String)entry.getKey());
            }
        }
        return centers;
    }

    public int calSimilarity(HashMap<String,Double> sum,HashMap<String,Double>center){
        int share = 0;
        Iterator it = sum.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry e = (Map.Entry)it.next();
            String  k =(String)e.getKey();
            if(center.containsKey(k)){
                share++;
            }
        }
        return share;
    }

    public void calCenter(double count){
        Iterator iter = sum.entrySet().iterator();
        Map.Entry e = (Map.Entry)iter.next();
        String k = (String)e.getKey();
        Double v = (Double)e.getValue();
        sum.put(k,v/count);
    }

    public Vector<String>getRaterIDs(String vec){
        int start = 0;
        int end = -1;
        Vector<String> rids = new Vector<>();
        while((end = vec.indexOf(":",start))!=-1){
            rids.add(vec.substring(start,end));
            start = vec.indexOf(",",end);
            if(start==-1) break;
            start++;
        }
        return rids;
    }

    public Vector<Double>getRates(String vec){
        Vector<Double> rates = new Vector<>();
        int start = vec.indexOf(":");
        start++;
        int end = -1;
        while((end= vec.indexOf(",",start))!=-1){
            rates.add(Double.parseDouble(vec.substring(start,end)));
            start = vec.indexOf(":",end);
            start++;
        }
        rates.add(Double.parseDouble(vec.substring(start)));
        return rates;
    }

    public void updateSum(Vector<String>rids,Vector<Double>rates){
        //System.err.println("rid_size="+rids.size()+" rate_size="+rates.size());
        for(int i = 0;i<rids.size();i++){
            sum.put(rids.get(i),rates.get(i));
        }
    }

    public List map2List(HashMap<String,Double>sum){
        List <Map.Entry<String,Double>> list = new ArrayList<>(sum.entrySet());
        Collections.sort(list,new Comparator<Map.Entry<String,Double>>(){
            @Override
            public int compare(Map.Entry<String,Double> o1, Map.Entry<String,Double> o2){
                return o2.getValue().compareTo(o1.getValue());
            }
        });
        return list;
    }

    public String vec2String(List<Map.Entry<String,Double>>list){
        //截出前2000个rate最大的
        String ans = new String();
        int size = list.size()<2000?list.size():2000;
        for(int i = 0;i<size;i++){
            ans+=list.get(i).getKey()+":"+list.get(i).getValue()+",";
        }
        ans = ans.substring(0,ans.length()-1);
        return ans;
    }


    @Override
    public void setup(Context context)throws IOException,InterruptedException{
        System.err.println("start reducing.....");
        String canopyCenterFileName = context.getConfiguration().get("canopycenter");
        System.err.println("canopy center file name is:"+canopyCenterFileName);
        BufferedReader reader = new BufferedReader(new FileReader(canopyCenterFileName));
        String line = reader.readLine();
        while(line!=null){
            String cid = getCid(line);
            HashMap<String,Double> vec = getVec(line);
            canopyCenter.put(cid,vec);
            line = reader.readLine();
        }
        System.err.println("reducer setup successfully!");
    }

    @Override
    public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException{
        int value_num=0;
        sum.clear();
        HashMap<String,Double> center = new HashMap<>();
        for(Text val:values){
            value_num++;
            String vec = val.toString();
            Vector<String>rids = getRaterIDs(vec);
            Vector<Double>rates = getRates(vec);
            updateSum(rids,rates);
        }
        calCenter(value_num);
        List list = map2List(sum);

        String vec =vec2String(list);
        Vector<String>cids = updateCanopyCenter();
        String value = vec+"|||";
        for(int i = 0;i<cids.size();i++){
            value+=cids.get(i)+",";
        }
        value=value.substring(0,value.length()-1);
        context.write(key,new Text(value));
    }
}
