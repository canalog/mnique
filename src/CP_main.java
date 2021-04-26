import java.sql.*;
import java.util.*;

public class CP_main {
	static int m = 1;
	static String otable = "NHIS_10000";
	static String rtable = "r4";
	static String id = "row_id";
	static int terminateCond = 50;
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		MariaDBConnect db = new MariaDBConnect();
		db.connect();
		
		HashMap<Integer,HashMap<String,String>> o_datas = new HashMap<Integer,HashMap<String,String>>();
		HashMap<Integer,HashMap<String,String>> r_datas = new HashMap<Integer,HashMap<String,String>>();
			
		o_datas = MariaDBConnect.get_data("NHIS_10000");
		r_datas = MariaDBConnect.get_data("R1");
	
		HashMap<String,Double> attribute = new HashMap<String,Double>();

		HashMap<String, HashMap<String,Double>> attribute_values = new HashMap<String,HashMap<String,Double>>();
		
		attribute_values = CalculateValues("NHIS_10000");

//		Iterator<String> properties = attribute_values.keySet().iterator();
//		while(properties.hasNext()) {
//			String key = properties.next();
//			System.out.println(key+" "+attribute_values.get(key));
//		}
//		System.out.println();
		attribute = CalculateAttributes(attribute_values);
//		
//		Iterator<String> a = attribute.keySet().iterator();
//		while(a.hasNext()) {
//			String key = a.next();
//			System.out.println(key+" "+attribute.get(key));
//		}
		
		List<String> arranged_attr = new ArrayList<>();
		arranged_attr = arrange(attribute);
		
		Iterator<String> a = arranged_attr.iterator();
		String[] attr = new String[21];
		boolean[] visited = new boolean[21];
		int i = 0;
		while(a.hasNext()) {
			visited[i] = false;
			attr[i] = a.next();
			System.out.println(attr[i]);
			i++;
		}
		
		ArrayList<Integer> mm = new ArrayList<Integer>();
		HashMap<Integer, ArrayList<String>> reid = new HashMap<Integer, ArrayList<String>>();
		
		for(int j = 1;j <= 21;j++) {
			comb(attr, visited, 0, 21, j, reid);
		}
		
		Iterator<Integer> reidi = reid.keySet().iterator();
		while(reidi.hasNext()) {
			int key = reidi.next();
			System.out.println("re-identified id : "+key+", attribute : "+reid.get(key));
		}
	}
	
	public static void comb(String[] arr, boolean[] visited, int start, int n, int r, HashMap<Integer, ArrayList<String>> reid) throws SQLException {
		if(reid.size() >= terminateCond) return;
		if(r == 0) {
			ArrayList<String> attr = new ArrayList<String>();
			for(int i=0;i<arr.length;i++) {
				if(visited[i]){
					attr.add(arr[i]);
					System.out.print(arr[i]+" ");
				}
			}
			System.out.println("");
			
			ArrayList<Integer> om = new ArrayList<Integer>();
			ArrayList<Integer> rm = new ArrayList<Integer>();
			String attrStr = makeAttrString(attr);
			
			// ���� �����ͼ¿��� m+1 ���ϼ��� �������� �ʴ�(���� �Ӽ����� ���� ���ڵ尡 m�� ������) ���ڵ� ã��
			ResultSet rs = MariaDBConnect.stmt.executeQuery("select count(*),"+ attrStr +" from "+otable+" group by "+attrStr);
			while(rs.next()) {
				if(rs.getInt(1) <= m) {
					String[] cut = new String[attr.size()];
					String v = "";
					String[] attrVal = new String[attr.size()];
					for(int i=0;i<attrVal.length;i++){
						v = rs.getString(2+i);
						if(v.indexOf(".") != -1) {
		              		  cut[i] = v.substring(v.indexOf(".")+1);
		              	  	}
						attrVal[i] = v;
					}
					ResultSet rs2 = MariaDBConnect.stmt.executeQuery("select "+id+" from "+otable+" where "+makeEqualString(attr, attrVal, cut));
					while(rs2.next()) {
						om.add(rs2.getInt(1));
					}
				}
			}
			
			// ��ĺ� �����ͼ¿��� m+1 ���ϼ��� �������� �ʴ�(���� �Ӽ����� ���� ���ڵ尡 m�� ������) ���ڵ� ã��
			rs = MariaDBConnect.stmt.executeQuery("select count(*),"+ attrStr +" from "+rtable+" group by "+attrStr);
			while(rs.next()) {
				if(rs.getInt(1) <= m) {
					String[] cut = new String[attr.size()];
					String v = "";
					String[] attrVal = new String[attr.size()];
					for(int i=0;i<attrVal.length;i++){
						v = rs.getString(2+i);
						if(v.indexOf(".") != -1) {
		              		  cut[i] = v.substring(v.indexOf(".")+1);
		              	  	}
						attrVal[i] = v;
					}
					ResultSet rs2 = MariaDBConnect.stmt.executeQuery("select "+id+" from "+rtable+" where "+makeEqualString(attr, attrVal, cut));
					while(rs2.next()) {
						rm.add(rs2.getInt(1));
					}
				}
			}
			
			// �� ���� �����ͼ¿��� ã�� ���ڵ� �� id�� ���� ���ڵ带 ã�� �Ӽ��� �� (���赵 ���� ��)
			// ������ ��ĺ� �Ǵ� ���ڵ��̹Ƿ� mm�� ����
			for(int i=0;i<om.size();i++) {
				if(rm.indexOf(om.get(i)) != -1) {
					// TODO: id ������ continue �ϱ�
					if(reid.containsKey(om.get(i))) continue;
					
					rs = MariaDBConnect.stmt.executeQuery("select "+ attrStr +" from "+ rtable +" where "+id+"="+om.get(i));
					rs.next();
					String[] rAttr = new String[attr.size()];
					for(int j = 0;j < rAttr.length;j++){
						rAttr[j] = rs.getString(1+j);
					}
					
					rs = MariaDBConnect.stmt.executeQuery("select "+ attrStr +" from "+ otable +" where "+id+"="+om.get(i));
					rs.next();
					String[] oAttr = new String[attr.size()];
					for(int j = 0;j < oAttr.length;j++){
						oAttr[j] = rs.getString(1+j);
					}
					
					if(!reid.containsKey(om.get(i))) {
						boolean isSame = true;
						for(int j = 0;j<rAttr.length;j++) {
							if(!rAttr[j].equals(oAttr[j])) {
								isSame = false;
								break;
							}
						}
						// id ����
						if(isSame) {
							if(reid.size() >= terminateCond) return;
							// mm.add(om.get(i));
							reid.put(om.get(i), attr);
						}
					}
				}
			}
			
//			System.out.println(attrStr + " " + mm.size());
//			if(mm.size()<10)
//			for(int i=0;i<mm.size();i++) {
//				System.out.println(mm.get(i));
//			}
			
			return;
		}
		for(int i=start;i<n;i++) {
			visited[i] = true;
			comb(arr, visited, i+1, n, r-1, reid);
			visited[i] = false;
		}
	}
	
	public static List<String> arrange(HashMap<String,Double> attribute){
		List<String> list = new ArrayList<>();
		list.addAll(attribute.keySet());
		Collections.sort(list,new Comparator() {
			@Override
			public int compare(Object o1, Object o2) {
				Object v1 = attribute.get(o1);
				Object v2 = attribute.get(o2);
				// TODO Auto-generated method stub
				return ((Comparable)v2).compareTo(v1);
			}
			
		});
//		Iterator it = list.iterator();
//
//		while(it.hasNext()) {
//			String temp = (String) it.next();
//			System.out.println(temp+" = "+attribute.get(temp));
//		}
		return list;
	}

	
	// sql query�� �� �Ӽ� string �����
	// return : attr_name1, attr_name2, ... , attr_nameN
	public static String makeAttrString(ArrayList<String> attr) {
		String ret = attr.get(0);
		for(int i=1;i<attr.size();i++) {
			ret += ", " + attr.get(i);
		}
		
		return ret;
	}
	
	public static String makeEqualString(ArrayList<String> attr, String[] attrVal, String[] cut) {
		String ret = " = " + attrVal[0];
		if(cut[0] != null) {
			ret = "round("+attr.get(0)+", "+cut[0].length()+")" + ret;
		}
		else {
			ret = attr.get(0)+ ret;
		}
		for(int i=1;i<attr.size();i++) {
			ret += " AND ";
			if(cut[i] != null) {
				ret += "round("+attr.get(i)+", "+cut[i].length()+")";
			}
			else {
				ret += attr.get(i);
			}
			ret += "=" + attrVal[i];
		}
		return ret;
	}

	
	public void bruteForce(HashMap<String, Double> attribute, MariaDBConnect db) throws SQLException{
		// Brute-force
		int m = 1;
		
		// 1-d
		String otable = "NHIS_10000";
		String rtable = "r4";
		String id = "row_id";
		Iterator<String> a = attribute.keySet().iterator();
		while(a.hasNext()) {
			ArrayList<Integer> om = new ArrayList<Integer>();
			ArrayList<Integer> rm = new ArrayList<Integer>();
			ArrayList<Integer> mm = new ArrayList<Integer>();
			String attr = a.next();
			// ���� �����ͼ¿��� m+1 ���ϼ��� �������� �ʴ�(���� �Ӽ����� ���� ���ڵ尡 m�� ������) ���ڵ� ã��
			ResultSet rs = db.executeQuery("select count(*),"+ id +" from "+otable+" group by "+attr);
			while(rs.next()) {
				if(rs.getInt(1) <= m) om.add(rs.getInt(2));
			}
			
			// ��ĺ� �����ͼ¿��� m+1 ���ϼ��� �������� �ʴ�(���� �Ӽ����� ���� ���ڵ尡 m�� ������) ���ڵ� ã��
			rs = db.executeQuery("select count(*),"+ id +" from "+rtable+" group by "+attr);
			while(rs.next()) {
				if(rs.getInt(1) <= m) rm.add(rs.getInt(2));
			}
			
			// �� ���� �����ͼ¿��� ã�� ���ڵ� �� id�� ���� ���ڵ带 ã�� �Ӽ��� ��
			// ������ ��ĺ� �Ǵ� ���ڵ��̹Ƿ� mm�� ����
			for(int i=0;i<om.size();i++) {
				if(rm.indexOf(om.get(i)) != -1) {
					rs = db.executeQuery("select "+ attr +" from "+ rtable +" where "+id+"="+om.get(i));
					rs.next();
					String rAttr = rs.getString(1);
					
					rs = db.executeQuery("select "+ attr +" from "+ otable +" where "+id+"="+om.get(i));
					rs.next();
					String oAttr = rs.getString(1);
					if(rAttr.equals(oAttr)) {
						mm.add(om.get(i));
					}
				}
//				for(int j=0;j<rm.size();j++) {
//					if(om.get(i) == rm.get(j)) {
//						
//					}
//				}
			}
			
//			System.out.println(attr + " " + mm.size());
//			if(mm.size()<10)
//			for(int i=0;i<mm.size();i++) {
//				System.out.println(mm.get(i));
//			}
		}
	}
	
	public static HashMap<String, HashMap<String,Double>>CalculateValues(String table) throws SQLException {
		String sql = "select count(*) from "+table;
		MariaDBConnect.rs =  MariaDBConnect.stmt.executeQuery(sql);
		int data_num = 0;
		while( MariaDBConnect.rs.next()) {
			data_num = MariaDBConnect.rs.getInt(1);
		}
		
		sql = "select * from "+table;
		ArrayList<String> properties = new ArrayList<String>();
		
		 MariaDBConnect.rs = MariaDBConnect.stmt.executeQuery(sql);
		 MariaDBConnect.rsmd =  MariaDBConnect.rs.getMetaData();
		int property_num =  MariaDBConnect.rsmd.getColumnCount();
		
		for (int i = 1; i <= property_num; i++) {
			if(! MariaDBConnect.rsmd.getColumnName(i).contains("ID"))
			{properties.add( MariaDBConnect.rsmd.getColumnName(i));}
		}
		
		HashMap<String, HashMap<String,Double>> attribute_values = new HashMap<String,HashMap<String,Double>>();
		
		for(int i=0;i<properties.size();i++) {
			HashMap<String,Double> values = new HashMap<String,Double>();
			sql = "select "+properties.get(i)+", count(*) from "+table+" group by "+properties.get(i);
			 MariaDBConnect.rs = MariaDBConnect.stmt.executeQuery(sql);
			while( MariaDBConnect.rs.next()) {
				values.put( MariaDBConnect.rs.getString(1),  1/MariaDBConnect.rs.getDouble(2));
			}
			attribute_values.put(properties.get(i), values);
		}
		return attribute_values;
	}
	public static HashMap<String,Double> CalculateAttributes(HashMap<String,HashMap<String,Double>> attribute_values) {
		HashMap<String,Double> attribute = new HashMap<String,Double>();
		
		Iterator<String> properties = attribute_values.keySet().iterator();
		while(properties.hasNext()) {
			String key = properties.next();
			Double values = 1.0;
			Iterator<String> p = attribute_values.get(key).keySet().iterator();
			int size = attribute_values.get(key).size();
			while(p.hasNext()) {
				double m  =  attribute_values.get(key).get(p.next());
				if(values > m)
				{
					values = m;
				}
			}
			attribute.put(key,values);
		}
		return attribute;
	}
}
