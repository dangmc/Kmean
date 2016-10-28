package VCC_ASS.MapReduce;

import java.awt.geom.Point2D;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Vector;

public class GeneratePoint {
	
	public static final int MAXN = 30000;
	public static final int NUMBERPOINTINCLUSTER1 = 10000;
	public static final int NUMBERPOINTINCLUSTER2 = 8000;
	public static final int NUMBERPOINTINCLUSTER3 = 12000;
	public static final int R1 = 400;
	public static final int R2 = 500;
	public static final int R3 = 600;
	public static Point2D centroid1;
	public static Point2D centroid2;
	public static Point2D centroid3;
	
	
	public static Point2D A, B, C;
	public static Vector<Point2D> points;
	
	public static void init() {
		A = new Point2D.Double(800, 800);
		B = new Point2D.Double(4000, 800);
		C = new Point2D.Double(2400, 2400);
		points = new Vector<Point2D>();
	}
	
	public static void Generate(int numberPoint, Point2D center, int radius) {
		for (int i = 0; i < numberPoint; i++) {
			double x = Randomize.nextDouble() * 2 - 1;
			double yMin = - Math.sqrt(1 - x * x);
			double yMax = Math.sqrt(1 - x * x);
			double y = Randomize.nextDouble() * (yMax - yMin) + yMin;
			
			Point2D p = new Point2D.Double();
			p.setLocation(x * radius + center.getX(), y * radius + center.getY());
			points.add(p);
		}
	}
	
	public static void main(String []args) throws IOException {
		Randomize.Init();
		Randomize.setSeed(1);
		init();
		Generate(NUMBERPOINTINCLUSTER1, A, R1);
		Generate(NUMBERPOINTINCLUSTER2, B, R2);
		Generate(NUMBERPOINTINCLUSTER3, C, R3);
		Collections.shuffle(points);
		File file = new File("output4.txt");
		FileWriter fw = new FileWriter(file);
		BufferedWriter bw = new BufferedWriter(fw);
		
		for (Point2D p : points) {
			bw.write(p.getX() + " " + p.getY() + "\n");
			//System.out.println("Point " + cnt + "-th: " + "(" + p.getX() + ", " + p.getY() + ")");
		}
		bw.close();
		fw.close();
		
		int index = Randomize.nextInt(MAXN);
		centroid1 = points.get(index);	
		
		index = Randomize.nextInt(MAXN);
		centroid2 = points.get(index);	
		
		index = Randomize.nextInt(MAXN);
		centroid3 = points.get(index);	
		
		file = new File("centroid.txt");
		fw = new FileWriter(file);
		bw = new BufferedWriter(fw);
		bw.write(centroid1.getX() + "," + centroid1.getY() + "\n");
		bw.write(centroid2.getX() + "," + centroid2.getY() + "\n");
		bw.write(centroid3.getX() + "," + centroid3.getY() + "\n");
		bw.close();
		fw.close();
	}
	
}
