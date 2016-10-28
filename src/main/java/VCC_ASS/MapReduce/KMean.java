package VCC_ASS.MapReduce;


import java.io.IOException;
import java.util.*;
import java.awt.geom.Point2D;
import java.io.*;

import org.apache.commons.collections.comparators.ComparableComparator;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.tools.GetConf;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

@SuppressWarnings("deprecation")
public class KMean {
	public static String CENTROID_FILE_NAME = "/centroid.txt";
	public static String OUTPUT_FILE_NAME = "/part-r-00000";
	public static String DATA_FILE_NAME = "/output4.txt";
	public static String JOB_NAME = "KMean";
	public static double esp = 0.0001;
	public static int K = 3;
	public static List<Point2D> centroids = new ArrayList<Point2D>();
	public static List<Point2D> newCentroids = new ArrayList<Point2D>();
	

	/*---------------------------------------Mapper------------------------------------------------------------------------*/
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		
		Vector<Point2D> centroids = new Vector<Point2D>();
		public void setup(Context context) {
			try {
				Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				if (cacheFiles != null && cacheFiles.length > 0) {
					String line;
					centroids.clear();
					BufferedReader cacheReader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
					try {
						while ((line = cacheReader.readLine()) != null) {
							String[] temp = line.split(",");
							Point2D centroid = new Point2D.Double();
							centroid.setLocation(Double.parseDouble(temp[0]), Double.parseDouble(temp[1]));
							centroids.add(centroid);
						}
					} finally {
						cacheReader.close();
					}
				}
			} catch (IOException e) {
				System.err.println("Exception reading DistribtuedCache: " + e);
			}
		}

		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String []coordinate = line.split(" ");
			Point2D point = new Point2D.Double();
			point.setLocation(Double.parseDouble(coordinate[0]), Double.parseDouble(coordinate[1]));
			
			Point2D centroidNearest = centroids.firstElement();
			double minD = Double.MAX_VALUE;
			for (Point2D centroid : centroids) {
				double distance = centroid.distance(point);
				if (distance < minD) {
					minD = distance;
					centroidNearest = centroid;
				}
			}
			context.write(new Text(centroidNearest.getX() + "," + centroidNearest.getY()), value);
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Point2D newCentroid;
			double sumX = 0;
			double sumY = 0;
			int numberPoints = 0;
			for(Text value : values) {
				String  point = value.toString();
				String []coordinate = point.split(" ");
				sumX += Double.parseDouble(coordinate[0]);
				sumY += Double.parseDouble(coordinate[1]);
				numberPoints++;
			}

			// We have new center now
			newCentroid = new Point2D.Double(sumX / numberPoints, sumY / numberPoints);

			// Emit new center and point
			context.write(new Text(newCentroid.getX() + "," + newCentroid.getY()), new Text());
		}
	}

	public static void readFileSystem(FileSystem fs, Path ofile, List<Point2D> list) throws IOException {
		list.clear();
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(ofile)));
		String line = br.readLine();
		while (line != null) {
			String[] coordinate = line.split(",");
			Point2D point = new Point2D.Double(Double.parseDouble(coordinate[0]), Double.parseDouble(coordinate[1]));
			list.add(point);
			line = br.readLine();
		}
		br.close();
		
		Collections.sort(list, new Comparator<Point2D>() {
			public int compare(Point2D p1, Point2D p2) {
				if (p1.getX() < p2.getX()) return -1;
			    if (p1.getX() > p2.getX()) return 1;
			    if (p1.getY() < p2.getY()) return -1;
			    if (p1.getY() > p2.getY()) return 1;
			    return 0;
			}
		});
	}
	
	public static void main(String[] args) throws Exception {
		String input = args[0];
		String output = args[1];
		String centroidsFile = args[2] + CENTROID_FILE_NAME;
		
		Path ofile = new Path(centroidsFile);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		readFileSystem(fs, ofile, centroids);
		// Reiterating till the convergence
		int iteration = 0;
		boolean isdone = false;
		while (isdone == false) {
			conf = new Configuration();
			if (iteration == 0) {
				Path hdfsPath = new Path(centroidsFile);
				DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
			} else {
				Path hdfsPath = new Path(output + Integer.toString(iteration - 1) + OUTPUT_FILE_NAME);
				DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
			}
			Job job = new Job(conf, JOB_NAME);
			fs = FileSystem.get(conf);
			//fs.delete(new Path(output), true);
			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output + Integer.toString(iteration)));
			job.setJarByClass(KMean.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			job.waitForCompletion(true);
			ofile = new Path(output + Integer.toString(iteration) + OUTPUT_FILE_NAME);
			fs = FileSystem.get(conf);
			readFileSystem(fs, ofile, newCentroids);
			int cnt = 0;
			for (int i = 0; i < newCentroids.size(); i++) {
				Point2D oldCentroid = centroids.get(i);
				Point2D newCentroid = newCentroids.get(i);
				if (oldCentroid.distance(newCentroid) < esp) cnt++;
				centroids.set(i, newCentroid);
			}
			for (Point2D centroid : centroids) {
				System.out.println(centroid.getX() + "," + centroid.getY());
			}
			if (cnt == K) isdone = true;
			iteration++;
		}
	}
}