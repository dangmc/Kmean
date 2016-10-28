package VCC_ASS.MapReduce;

import java.util.Random;

public class Randomize {
	public static Random rand;
	
	public static void  Init() {
		rand = new Random();
	}
	
	public static void setSeed(long seedNumber) {
		rand.setSeed(seedNumber);
	}
	
	public static int nextInt(int upperBound){
		return rand.nextInt(upperBound);
	}
	
	public static double nextDouble() {
		return rand.nextDouble();
	}
}
