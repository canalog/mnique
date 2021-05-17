import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;


public class MariaDBConnect {
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
			
		//resultsets = get the result of sql queries

		HashMap<Integer,HashMap<String,String>> o_datas = new HashMap<Integer,HashMap<String,String>>();
		HashMap<Integer,HashMap<String,String>> r_datas = new HashMap<Integer,HashMap<String,String>>();
			
		// o_datas = get_data("NHIS_10000");
		//r_datas = get_data("R1");

//		for(Integer i : o_datas.keySet()) {
//			System.out.println("Key: "+i+" Values: "+o_datas.get(i));
//		}

		for(Integer i : r_datas.keySet()) {
			System.out.println("Key: "+i+" Values: "+r_datas.get(i));
		}
	}
	
	public void connect() throws SQLException {
		try {
			Class.forName("org.mariadb.jdbc.Driver");
		}
		catch(ClassNotFoundException e){
			e.printStackTrace();
		}
		//connect DB(나중에 알아서 받아)
		conn = DriverManager.getConnection("jdbc:mysql://localhost:3306","root","root");
		System.out.println("Connection Success!");
			
		//create sql statements
		stmt = conn.createStatement();
		stmt.executeUpdate("use NHIS");
	}
	
	public static String primaryKey(String table) throws SQLException {
		ArrayList<String> properties = new ArrayList<String>();
		String sql = "desc ";
		
		String pk = "";
		
		rs = stmt.executeQuery(sql+table);
		
		// 일반화할 때 다 바꿔야 함. 
		while(rs.next()) {
			properties.add(rs.getString(1));
			if(rs.getString(4).equals("PRI")) {
				pk = rs.getString(1);
			}
		}
		pk = pk.toLowerCase();

		return pk;
	}

	public static HashMap<String, Double> getNumberColumns(String table) throws SQLException {
		// std
		rsmd = stmt.executeQuery("select * from " + table).getMetaData();
		// big int : -5, bit : -7, decimal : 3, double : 8, float : 6, integer : 4, tiny int : -6
		// real : 7, numeric : 2, smallint : 5
		List<Integer> numtype = Arrays.asList(-5, -7, 3, 8, 6, 4, -6, 7, 2, 5);

		HashMap<String, Double> std = new HashMap<String, Double>();
		int coln = rsmd.getColumnCount();
		for (int i = 1; i <= coln; i++) {
			if (numtype.contains(rsmd.getColumnType(i))) {
				std.put(rsmd.getColumnName(i).toLowerCase(), 0.0);
			}
		}
		return std;
	}
	public static HashMap<String, Double> getSTD(HashMap<String, Double> std) throws SQLException {
		
		rsmd = stmt.executeQuery("select * from nhis_10000").getMetaData();
		
		for (String i : std.keySet()) {
			rs = stmt.executeQuery("SELECT STD(" + i + ") FROM nhis_10000");
			rs.next();
			std.put(i, rs.getDouble(1));
		}
		return std;
	}

	public static HashMap<Integer, HashMap<String, Double>> get_data(String table) throws SQLException {

		rs = stmt.executeQuery("select * from "+table);
		rsmd = rs.getMetaData();
		int property_num = rsmd.getColumnCount();
		ArrayList<String> properties = new ArrayList<String>();
		for (int i = 1; i <= property_num; i++) {
			properties.add(rsmd.getColumnName(i));
		}
		
		String pk = primaryKey("NHIS_10000");
		
		HashMap<Integer,HashMap<String,Double>> Map = new HashMap<Integer,HashMap<String,Double>>();
		
		String sql2 = "select * from ";
		rs = stmt.executeQuery(sql2 + table);
		
		int count = 0;
		HashMap<String,Double> p;
		String s;
		while(rs.next()) {
			p = new HashMap<String,Double>();
			for(int i=1;i<=property_num;i++) {
				s = properties.get(i-1).toLowerCase();
				if(s.equals(pk)) {
					count = Integer.parseInt(rs.getString(i));
				} else {
					p.put(s,rs.getDouble(i));
				}
			}
			Map.put(count, p);
		}
		return Map;
	}
	
	public ResultSet executeQuery(String query) throws SQLException{
		return stmt.executeQuery(query);
	}
}
