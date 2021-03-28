import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class MariaDBConnect {
	
	public static Map<String, Object> get_table(Statement stmt, String table) throws SQLException {
		try {
			Map<String, Object> tableMap = new HashMap<String, Object>();	
			//resultsets = get the result of sql queries
			ResultSet rs = stmt.executeQuery("select * from " + table);
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnCnt = rsmd.getColumnCount();
			while(rs.next()) {
				for (int i = 1; i <= columnCnt; i++) {
					tableMap.put(rsmd.getColumnName(i), rs.getString(rsmd.getColumnName(i)));
				}
	//			System.out.println(rs.getString(1)+"\t"+rs.getInt(2)+"\t"+rs.getString(3)+"\t"
	//		+rs.getString(4)+"\t"+rs.getInt(5));
			}
			return tableMap;
		}
		catch(SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		final String driver = "org.mariadb.jdbc.Driver";
		final String DB_IP = "localhost";
		final String DB_PORT = "3306";
		final String DB_NAME = "nhis";
		final String DB_URL = 
				"jdbc:mariadb://" + DB_IP + ":" + DB_PORT + "/" + DB_NAME;
		
		try {
			Class.forName("org.mariadb.jdbc.Driver");
		}
		catch(ClassNotFoundException e){
			e.printStackTrace();
		}
		
		Connection conn = null;	
		Statement stmt = null;
		
		try {
			//connect DB
			conn = DriverManager.getConnection("jdbc:mariadb://localhost:3306","root","password");
			System.out.println("Connection Success!");
			
			//create sql statements
			stmt = conn.createStatement();
			stmt.executeUpdate("use nhis");
			
			Map<String, Object> nhis_10000 = get_table(stmt, "nhis_10000");
			Map<String, Object> r1 = get_table(stmt, "r1");
			r1.forEach((key, value) -> System.out.println(key + ":" + value));
//			Map<String, Object> original = new HashMap<String, Object>();
//			
//			//resultsets = get the result of sql queries
//			ResultSet rs = stmt.executeQuery("select * from nhis_10000");
//			ResultSetMetaData rsmd = rs.getMetaData();
//			int columnCnt = rsmd.getColumnCount();
//			while(rs.next()) {
//				for (int i = 1; i <= columnCnt; i++) {
//					original.put(rsmd.getColumnName(i), rs.getString(rsmd.getColumnName(i)));
//				}
////				System.out.println(rs.getString(1)+"\t"+rs.getInt(2)+"\t"+rs.getString(3)+"\t"
////			+rs.getString(4)+"\t"+rs.getInt(5));
//			}
//			original.forEach((key, value) -> System.out.println(key + ":" + value));
		}
		catch(SQLException e) {
			e.printStackTrace();
		}

	}
}
