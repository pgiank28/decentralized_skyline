package hadoop.skyline;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class angleMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>{

	private float p = (float)Math.PI;
	private float [] fi2d2n=new float[3];
	private float [] fi2d3n=new float[4];
	private float [] fi2d4n=new float[5];
	private float [] fi2d5n=new float[6];
	private float [] fi2d6n=new float[7];
	private float [] fi2d7n=new float[8];
	private float [] fi2d8n=new float[9];
	private float [] fi2d9n=new float[10];
	private float [] fi2d10n=new float[11];
	private float [] fi3d9n_a=new float[18];
	private float [] fi3d9n_b=new float[18];
	int first=0;
	int dimensions;
	int partitions;

	public void configure(JobConf jb){
    //**
    // Both 2-d and 3-d tuples are supported
    // **
    
		String d=jb.get("dimensions");
		if(d.equals("2d")){
			dimensions=2;
		}
		if(d.equals("3d")){
			dimensions=3;
		}
		partitions=Integer.parseInt(jb.get("partitions"));
	}
	public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> collector, Reporter reporter)
	throws IOException {
		if(first == 0){
			first = 1;
			initBoundaries();
			return;
		}
		String v = value.toString();
		//float ar=compute_r(v,dimensions);
		float afi[]=compute_fi(v,dimensions);
		int m=-1,partit=1;
		if(dimensions==3){
			m=10;
		}
		if(dimensions==2){
			m=partitions-1;
		}

		switch(m){
		case 1:partit=compute_partition_2d(fi2d2n,afi);
				break;
		case 2:partit=compute_partition_2d(fi2d3n,afi);
		break;
		case 3:partit=compute_partition_2d(fi2d4n,afi);
		break;
		case 4:partit=compute_partition_2d(fi2d5n,afi);
		break;
		case 5:partit=compute_partition_2d(fi2d6n,afi);
		break;
		case 6:partit=compute_partition_2d(fi2d7n,afi);
		break;
		case 7:partit=compute_partition_2d(fi2d8n,afi);
		break;
		case 8:partit=compute_partition_2d(fi2d9n,afi);
		break;
		case 9:partit=compute_partition_2d(fi2d10n,afi);
		break;
		case 10:partit=compute_partition_3d(fi3d9n_a,fi3d9n_b,afi);
		break;
		default:
			partit=0;
		}

		collector.collect(new IntWritable(partit), value);

	}

	public void initBoundaries(){

				fi2d2n[0]=0;
				fi2d2n[1]=p/4;
				fi2d2n[2]=p/2;

				fi2d3n[0]=0;
				fi2d3n[1]=p/6;
				fi2d3n[2]=p/3;
				fi2d3n[3]=p/2;

				fi2d4n[0]=0;
				fi2d4n[1]=p/8;
				fi2d4n[2]=p/4;
				fi2d4n[3]=3*p/8;
				fi2d4n[4]=p/2;

				fi2d5n[0]=0;
				fi2d5n[1]=p/10;
				fi2d5n[2]=p/5;
				fi2d5n[3]=3*p/10;
				fi2d5n[4]=4*p/10;
				fi2d5n[5]=p/2;

				fi2d6n[0]=0;
				fi2d6n[1]=p/12;
				fi2d6n[2]=p/6;
				fi2d6n[3]=3*p/12;
				fi2d6n[4]=4*p/12;
				fi2d6n[5]=5*p/12;
				fi2d6n[6]=p/2;

				fi2d7n[0]=0;
				fi2d7n[1]=p/14;
				fi2d7n[2]=p/7;
				fi2d7n[3]=3*p/14;
				fi2d7n[4]=4*p/14;
				fi2d7n[5]=5*p/14;
				fi2d7n[6]=3*p/7;
				fi2d7n[7]=p/2;

				fi2d8n[0]=0;
				fi2d8n[1]=p/16;
				fi2d8n[2]=p/8;
				fi2d8n[3]=3*p/16;
				fi2d8n[4]=p/4;
				fi2d8n[5]=5*p/16;
				fi2d8n[6]=6*p/16;
				fi2d8n[7]=7*p/16;
				fi2d8n[8]=p/2;

				fi2d9n[0]=0;
				fi2d9n[1]=p/18;
				fi2d9n[2]=p/9;
				fi2d9n[3]=3*p/18;
				fi2d9n[4]=4*p/18;
				fi2d9n[5]=5*p/18;
				fi2d9n[6]=p/3;
				fi2d9n[7]=7*p/18;
				fi2d9n[8]=8*p/18;
				fi2d9n[9]=p/2;

				fi2d10n[0]=0;
				fi2d10n[1]=p/20;
				fi2d10n[2]=p/10;
				fi2d10n[3]=3*p/20;
				fi2d10n[4]=p/5;
				fi2d10n[5]=p/4;
				fi2d10n[6]=6*p/20;
				fi2d10n[7]=7*p/20;
				fi2d10n[8]=8*p/20;
				fi2d10n[9]=9*p/20;
				fi2d10n[10]=p/2;

			fi3d9n_a[0]=0;
			fi3d9n_a[2]=0;
			fi3d9n_a[4]=0;
			fi3d9n_a[1]=(p/180)*(float)48.24;
			fi3d9n_a[3]=(p/180)*(float)48.24;
			fi3d9n_a[5]=(p/180)*(float)48.24;
			fi3d9n_a[6]=(p/180)*(float)48.24;
			fi3d9n_a[8]=(p/180)*(float)48.24;
			fi3d9n_a[10]=(p/180)*(float)48.24;
			fi3d9n_a[7]=(p/180)*(float)70.52;
			fi3d9n_a[9]=(p/180)*(float)70.52;
			fi3d9n_a[11]=(p/180)*(float)70.52;
			fi3d9n_a[12]=(p/180)*(float)70.52;
			fi3d9n_a[14]=(p/180)*(float)70.52;
			fi3d9n_a[16]=(p/180)*(float)70.52;
			fi3d9n_a[13]=p/2;
			fi3d9n_a[15]=p/2;
			fi3d9n_a[17]=p/2;

			//fi2 measurements
			fi3d9n_b[0]=0;
			fi3d9n_b[2]=0;
			fi3d9n_b[4]=0;
			fi3d9n_b[1]=p/6;
			fi3d9n_b[3]=p/6;
			fi3d9n_b[5]=p/6;
			fi3d9n_b[6]=p/6;
			fi3d9n_b[8]=p/6;
			fi3d9n_b[10]=p/6;
			fi3d9n_b[7]=p/3;
			fi3d9n_b[9]=p/3;
			fi3d9n_b[11]=p/3;
			fi3d9n_b[12]=p/3;
			fi3d9n_b[14]=p/3;
			fi3d9n_b[16]=p/3;
			fi3d9n_b[13]=p/2;
			fi3d9n_b[15]=p/2;
			fi3d9n_b[17]=p/2;


	}

	public float compute_r(String xi,int d){
		StringTokenizer stra = new StringTokenizer(xi,",");
		stra.nextToken();

		float r=(float)0;

		if(d==2){
			String f=stra.nextToken();
			String g=stra.nextToken();
			float ff = Float.parseFloat(f);
			float gg = Float.parseFloat(g);
			r=(float) Math.sqrt((ff*ff)+(gg*gg));
		}
		if(d==3){
			String f=stra.nextToken();
			String g=stra.nextToken();
			String h=stra.nextToken();
			float ff = Float.parseFloat(f);
			float gg = Float.parseFloat(g);
			float hh = Float.parseFloat(h);
			r=(float) Math.sqrt((ff*ff)+(gg*gg)+(hh*hh));
		}
		return r;
	}

	public float[] compute_fi(String xi,int d){
		StringTokenizer strb = new StringTokenizer(xi,",");

		strb.nextToken();
		float fi[]=new float[2];
		fi[0]=(float)0;
		fi[1]=(float)0;
		if(d==2){
			String f=strb.nextToken();
			String g=strb.nextToken();
			float ff = Float.parseFloat(f);
			float gg = Float.parseFloat(g);
			fi[0]=(float)Math.atan(gg/ff);

		}
		if(d==3){
			String f=strb.nextToken();
			String g=strb.nextToken();
			String h=strb.nextToken();
			float ff = Float.parseFloat(f);
			float gg = Float.parseFloat(g);
			float hh = Float.parseFloat(h);
			fi[0]=(float)Math.atan(((float)Math.sqrt((gg*gg)+(hh*hh)))/ff);
			fi[1]=(float)Math.atan(gg/ff);

		}
		return fi;
	}

	public int compute_partition_3d(float[] binaries_1,float[] binaries_2,float[] fi){
		int d=0;
		int partition=0;
		while(partition<18){
			float down=binaries_1[partition];
			float upper=binaries_1[partition+1];

			if(down<fi[0] && fi[0]<=upper){
				if(binaries_2[partition]<fi[1] && fi[1]<=binaries_2[partition+1]){
					d = partition+1;
					break;
				}
			}
			partition=partition+2;
		}
		return d;
	}

	public int compute_partition_2d(float[] binaries,float[] fi){
		int partition=0;
		for(int i=0;i<binaries.length-1;i++){
			if(fi[0]>binaries[i] && fi[0]<binaries[i+1]){
				partition=i+1;
				break;
			}
		}
		return partition;
	}
}
