import java.sql.*;
import java.util.*;

public class CP_main {

	static Connection conn = null;	
	static Statement stmt = null;
	static ResultSet rs = null;
	static ResultSetMetaData rsmd = null;
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		try {
			Class.forName("org.mariadb.jdbc.Driver");
		}
		catch(ClassNotFoundException e){
			e.printStackTrace();
		}
		//connect DB(나중에 알아서 받아)
		conn = DriverManager.getConnection("jdbc:mysql://localhost:3306","root","1111");
		System.out.println("Connection Success!");
			
		//create sql statements
		stmt = conn.createStatement();
		stmt.executeUpdate("use NHIS");
		
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
		rs = stmt.executeQuery(sql);
		int data_num = 0;
		while(rs.next()) {
			data_num =rs.getInt(1);
		}
		
		sql = "select * from "+table;
		ArrayList<String> properties = new ArrayList<String>();
		
		rs = stmt.executeQuery(sql);
		rsmd = rs.getMetaData();
		int property_num = rsmd.getColumnCount();
		
		for (int i = 1; i <= property_num; i++) {
			if(!rsmd.getColumnName(i).contains("ID"))
			{properties.add(rsmd.getColumnName(i));}
		}
		
		HashMap<String, HashMap<String,Double>> attribute_values = new HashMap<String,HashMap<String,Double>>();
		
		for(int i=0;i<properties.size();i++) {
			HashMap<String,Double> values = new HashMap<String,Double>();
			sql = "select "+properties.get(i)+", count(*) from "+table+" group by "+properties.get(i);
			rs = stmt.executeQuery(sql);
			while(rs.next()) {
				values.put(rs.getString(1), rs.getDouble(2)/data_num);
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
