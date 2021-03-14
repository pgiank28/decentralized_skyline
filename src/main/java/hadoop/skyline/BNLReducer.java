package hadoop.skyline;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


import java.util.*;
public class BNLReducer extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>{

			LinkedList<String> window = new LinkedList<String>();
			private static final IntWritable one = new IntWritable(1);
			private int d;

	public void configure(JobConf jb){
		String dd=jb.get("dimensions");

		if(dd.equals("2d")){
			d=2;
		}
		if(dd.equals("3d")){
			d=3;
		}

	}
	public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> outputCollector,
			Reporter reporter) throws IOException {

			while(values.hasNext()){
			String inp = values.next().toString();
			if(window.size()==0){
				window.addFirst(inp);
			}else{
				int x=-1,tmp=0;
				int sz = window.size();
				while(tmp<sz){
					x = compareTwoParts(window.get(tmp),inp,d);
					if(x==0){
						tmp++;
					}
					if(x==1){
						break;
					}
					if(x==2){
						window.remove(tmp);
						sz--;
					}
				}
				if(tmp==sz){
					window.addFirst(inp);
				}
			}
			}

			for(int w=0;w<window.size();w++){
				outputCollector.collect(one, new Text(window.get(w)));
			}

			window.clear();
		}

		public int compareTwoParts(String a,String b,int dimensions){
			StringTokenizer sta = new StringTokenizer(a,",");
			StringTokenizer stb = new StringTokenizer(b,",");

			sta.nextToken();
			stb.nextToken();

			float p1=0,p2=0,a1=0,a2=0,d1=0,d2=0;

			/*Compare the tupples on price{p1,p2} age{a1,a2} and distance from city center {d1,d2}*/
		if(dimensions == 3){
			p1 = Float.parseFloat(sta.nextToken());
			a1 = Float.parseFloat(sta.nextToken());
			d1 = Float.parseFloat(sta.nextToken());


			p2 = Float.parseFloat(stb.nextToken());
			a2 = Float.parseFloat(stb.nextToken());
			d2 = Float.parseFloat(stb.nextToken());


			if(p1<p2 && a1<a2 && d1<d2){
				return 1; //element 2 eliminated
			}
			if(p1>p2 && a1>a2 && d1>d2){
				return 2; //element 1 eliminated
			}
			return 0; //incomparable types
		}

		/*Compare the tupples on age{a1,a2} and price {p1,p2}*/
		if(dimensions == 2){

			p1 = Float.parseFloat(sta.nextToken());
			a1 = Float.parseFloat(sta.nextToken());


			p2 = Float.parseFloat(stb.nextToken());
			a2 = Float.parseFloat(stb.nextToken());


			if(p1<p2 && a1<a2){
				return 1; //element 2 eliminated
			}
			if(p1>p2 && a1>a2){
				return 2; //element 1 eliminated
			}
			return 0; //incomparable types
		}
		return 0;
		}


}
