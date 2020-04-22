package com.ky.util;

public class SplitRegion {
	
	public static final String [] keys = {"1","2","3","4","5","6","7","8","9","A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z"};
	  
	public static byte[][] getSplitKey() {
		byte[][] splitKeys = new byte[keys.length * keys.length + 90][];
		String[] key = new String[keys.length * keys.length + 90];
		for (int i = 0; i < 90; i++) {
			splitKeys[i] = ("0" + (i + 10)).getBytes();
			key[i] = ("0" + (i + 10));
		}
		int k = 90;
		for (int i = 0; i < keys.length; i++) {
			for (int j = 0; j < keys.length; j++) {
				splitKeys[k] = (keys[i] + keys[j]).getBytes();
				k++;
			}
		}

		return splitKeys;
	} 
	
	/**
	 * 民航专用
	 * @return
	 */
	public static byte[][] getSplitKey70() {
		byte[][] splitKeys = new byte[70][];
		for (int i = 0; i < 70; i++) {
			splitKeys[i] = String.format("%02d", i).getBytes();
		}
		return splitKeys;
	}

	/**
	 * 旅馆住宿专用
	 * @return
	 */
	public static byte[][] getStaySplitKey() {
		byte[][] splitkeys = new byte[5][];
		splitkeys[0] = "43".getBytes();
		splitkeys[1] = "53".getBytes();
		splitkeys[2] = "54".getBytes();
		splitkeys[3] = "62".getBytes();
		splitkeys[4] = "65".getBytes();
		return splitkeys;
	}

	public static void main(String[] args) {
		getSplitKey();
	}
}
