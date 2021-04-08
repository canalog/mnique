import java.sql.*;
import java.util.*;

public class CP_main {

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		MariaDBConnect db = new MariaDBConnect();
		db.connect();
	
		HashMap<String,Double> attribute = new HashMap<String,Double>();

		HashMap<String, HashMap<String,Double>> attribute_values = new HashMap<String,HashMap<String,Double>>();
		
		attribute_values = CalculateValues("NHIS_10000");

		Iterator<String> properties = attribute_values.keySet().iterator();
		while(properties.hasNext()) {
			String key = properties.next();
			System.out.println(key+" "+attribute_values.get(key));
		}
		System.out.println();
		attribute = CalculateAttributes(attribute_values);
		
		Iterator<String> a = attribute.keySet().iterator();
		while(a.hasNext()) {
			String key = a.next();
			System.out.println(key+" "+attribute.get(key));
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
				values.put( MariaDBConnect.rs.getString(1),  MariaDBConnect.rs.getDouble(2)/data_num);
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
