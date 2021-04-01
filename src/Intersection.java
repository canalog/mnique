import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class Intersection {
	public static void main(String[] args) throws SQLException {

		MariaDBConnect db = new MariaDBConnect();
		db.connect();

		HashMap<Integer, HashMap<String, String>> original = db.get_data("NHIS_10000");
		HashMap<Integer, HashMap<String, String>> version1 = db.get_data("R1");

		HashMap<String, Double> std = db.getSTD(db.getNumberColumns("nhis_10000"));
		HashMap<String, Double> round = round(original);
		HashMap<Integer, HashMap<String, String>> intersect = intersection(original, version1, round);

	}

	public static HashMap<Integer, HashMap<String, String>> intersection(
			HashMap<Integer, HashMap<String, String>> original, HashMap<Integer, HashMap<String, String>> changed,
			HashMap<String, Double> threshold) {

		HashMap<Integer, HashMap<String, String>> intersect = new HashMap<Integer, HashMap<String, String>>();
		Iterator<Integer> keys = original.keySet().iterator();
		while (keys.hasNext()) {
			int key = keys.next();
			HashMap<String, String> o = original.get(key);
			HashMap<String, String> r = changed.get(key);
			if (r != null) {
				HashMap<String, String> t = new HashMap<String, String>();
				Iterator<String> ks = o.keySet().iterator();
				while (ks.hasNext()) {
					String k = ks.next();
					if (r.get(k) != null) {
						if (threshold.get(k) != null) {
							double on = Double.parseDouble(o.get(k));
							double rn = Double.parseDouble(r.get(k));
							if (on + threshold.get(k) >= rn && rn >= on - threshold.get(k)) {
								t.put(k, Double.toString(rn));
							}
						} else if (r.get(k).equals(o.get(k))) {
							t.put(k, r.get(k));
						}
					}
				}
				if (t.size() != 0)
					intersect.put(key, t);
			}
		}

		for (Integer i : intersect.keySet()) {
			System.out.println("attr.# : " + intersect.get(i).size() + " Key: " + i + " Values: " + intersect.get(i));
		}

		return changed;

	}
	
	public static HashMap<String,Double> round(HashMap<Integer,HashMap<String,String>> origin){
	   	ArrayList<String> o_attributes = new ArrayList<String>();
		
		HashMap<String,Double> threshold = new HashMap<String,Double>();
		
		Iterator<String> o_iter = origin.get(1).keySet().iterator();
		int idx = 1;
		
		while(o_iter.hasNext()) {
			String o_key = o_iter.next();
			o_attributes.add(o_key);
			String o_values = origin.get(idx).get(o_key);
			int len = 0;
			int check = o_values.indexOf(".");
			if(check >= 0) {
				len = o_values.substring(check+1,o_values.length()).length();
			}
			double d = Math.pow(10, -len) * 0.5;
			threshold.put(o_key,d);
			idx++;
		}
		return threshold;
	}
	
	public static HashMap<Integer, HashMap<String, String>> roundIntersection(
			HashMap<Integer, HashMap<String, String>> origin, HashMap<Integer, HashMap<String, String>> r) {

		HashMap<Integer, HashMap<String, String>> intersection = new HashMap<Integer, HashMap<String, String>>();
		HashMap<String, String> values;

		ArrayList<String> o_attributes = new ArrayList<String>();

		Iterator<String> o_iter = origin.get(1).keySet().iterator();
		while (o_iter.hasNext()) {
			String o_key = o_iter.next();
			o_attributes.add(o_key);
		}

		int count = 1;
		for (Integer i : r.keySet()) {
			values = new HashMap<String, String>();
			Iterator<String> r_iter = r.get(i).keySet().iterator();
			while (r_iter.hasNext()) {
				String r_key = r_iter.next();
				String r_values = r.get(i).get(r_key);
				int len = 0;
				if (o_attributes.contains(r_key)) {
					String o_values = origin.get(i).get(r_key);
					int index = o_values.indexOf(".");
					if (index >= 0) {
						len = o_values.substring(index + 1, o_values.length()).length();
					}
					double int_r = (Math.round(Float.parseFloat(r_values) * Math.pow(10, len)) / Math.pow(10, len));
					double int_o = Float.parseFloat(o_values);
					if (int_r == int_o) {
						if (r_key.equals("row_id")) {
							count = Integer.parseInt(r_values);
						} else {
							values.put(r_key, r_values);
						}
					}
				}
			}
			if (values.size() != 0) {
				intersection.put(count, values);
			}
		}
		return intersection;
	}
}
