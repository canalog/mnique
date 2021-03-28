import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;


public class MariaDBConnect {
	static Connection conn = null;	
	static Statement stmt = null;
	static ResultSet rs = null;

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
			
		//resultsets = get the result of sql queries

		HashMap<Integer,ArrayList<HashMap<String,String>>> o_datas = new HashMap<Integer,ArrayList<HashMap<String,String>>>();
		HashMap<Integer,ArrayList<HashMap<String,String>>> r_datas = new HashMap<Integer,ArrayList<HashMap<String,String>>>();
			
		o_datas = get_data("NHIS_10000");
		r_datas = get_data("R1");

		for(Integer i : o_datas.keySet()) {
			System.out.println("Key: "+i+" Values: "+o_datas.get(i));
		}
		for(Integer i : r_datas.keySet()) {
			System.out.println("Key: "+i+" Values: "+r_datas.get(i));
		}
	}
	public static HashMap<Integer,ArrayList<HashMap<String,String>>> get_data(String table) throws SQLException {
		int property_num = 0;
		String sql1 = "select count(*) from information_schema.columns where table_name = ";
		StringBuffer sb = new StringBuffer(table);
		sb.insert(0, "'");
		sb.insert(sb.length(), "'");
		
		rs = stmt.executeQuery(sql1+sb.toString());
		while(rs.next()) {
			property_num = rs.getInt(1);
		}
		
		ArrayList<String> properties = new ArrayList<String>();
		
		String sql = "desc ";
		
		String keyy = "";
		
		rs = stmt.executeQuery(sql+table);
		
		while(rs.next()) {
			properties.add(rs.getString(1));
			if(rs.getString(4).equals("PRI")) {
				keyy = rs.getString(1);
			}
		}
		keyy = keyy.toLowerCase();
		System.out.println(keyy);
		
		HashMap<Integer,ArrayList<HashMap<String,String>>> Map = new HashMap<Integer,ArrayList<HashMap<String,String>>>();
		
		String sql2 = "select * from ";
		rs = stmt.executeQuery(sql2 + table);
		
		ArrayList<HashMap<String, String>> values;
		HashMap<String,String> p;
		int count = 1;
		String s;
		while(rs.next()) {
			values = new ArrayList<HashMap<String,String>>();
			for(int i=1;i<=property_num;i++) {
				p = new HashMap<String,String>();
				s = properties.get(i-1).toLowerCase();
				p.put(s,rs.getString(i));
				values.add(p);
				if(s.equals(keyy)) {
					count = Integer.parseInt(rs.getString(i));
				}
			}
			Map.put(count, values);
		}
		return Map;
	}
}